package aramarchuk.keyValueBase.storage

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

class Replica(config: ReplicaConfig, val storage: WalBackedKVStore<String>):
    LwwMapSyncServer, LwwMapSyncClient {
    private val stateCodec = LwwMapStateBinaryCodec(
        encodeKey = { it.toByteArray(Charsets.UTF_8) },
        decodeKey = { it.toString(Charsets.UTF_8) },
    )
    private val server = HttpServer.create(
        InetSocketAddress(config.port),
        0
    )

    init {
        server.createContext("/snapshot") { exchange ->
            try {
                if (exchange.requestMethod != "POST") {
                    exchange.sendResponseHeaders(405, -1)
                    return@createContext
                }

                val incoming = stateCodec.decode(exchange.requestBody.readBytes())
                storage.merge(incoming)

                val responseBytes = stateCodec.encode(storage.snapshot())
                exchange.responseHeaders.set("Content-Type", "application/octet-stream")
                exchange.sendResponseHeaders(200, responseBytes.size.toLong())
                exchange.responseBody.use { it.write(responseBytes) }
            } catch (e: Exception) {
                exchange.sendResponseHeaders(500, -1)
            } finally {
                exchange.close()
            }
        }
    }

    override fun start() = server.start()

    override fun close() = server.stop(1)

    override fun sync(
        peer: URI,
    ) {
        val client = HttpClient.newHttpClient()
        val snapshot = stateCodec.encode(storage.snapshot())

        val request = HttpRequest.newBuilder()
            .uri(URI.create(peer.toString()))
            .POST(HttpRequest.BodyPublishers.ofByteArray(snapshot))
            .header("Content-Type", "application/octet-stream")
            .build()

        val response = client.send(
            request,
            HttpResponse.BodyHandlers.ofByteArray()
        )

        check(response.statusCode() == 200)
        storage.merge(stateCodec.decode(response.body()))
    }
}
