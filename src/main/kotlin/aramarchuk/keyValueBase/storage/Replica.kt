package aramarchuk.keyValueBase.storage

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.io.IOException
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

class Replica(config: ReplicaConfig) : AutoCloseable {
    val storage = WalBackedKVStore<String>(
        wal = FileWriteAheadLog(config.dataDirectory.resolve("store.wal")),
        encodeKey = { it.toByteArray(Charsets.UTF_8) },
        decodeKey = { it.toString(Charsets.UTF_8) },
        replicaId = config.replicaId,
    )

    private val stateCodec = MapBinaryCodec(
        encodeKey = { it.toByteArray(Charsets.UTF_8) },
        decodeKey = { it.toString(Charsets.UTF_8) },
        itemCodec = LwwRecordCodec,
    )
    private val httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(3))
        .build()
    private val server = try {
        HttpServer.create(InetSocketAddress(config.port), 0)
    } catch (failure: Throwable) {
        storage.close()
        throw failure
    }
    private var started = false
    private var closed = false

    init {
        server.createContext("/snapshot", ::handleSnapshot)
    }

    @Synchronized
    fun start() {
        check(!closed) { "Replica is closed" }
        check(!started) { "Replica is already started" }
        server.start()
        started = true
    }

    fun sync(peer: URI) {
        check(!closed) { "Replica is closed" }
        val snapshot = stateCodec.encode(storage.snapshot().entries)
        val request = HttpRequest.newBuilder(peer)
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/octet-stream")
            .header("Accept", "application/octet-stream")
            .POST(HttpRequest.BodyPublishers.ofByteArray(snapshot))
            .build()
        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray())

        if (response.statusCode() != 200) {
            val details = response.body().toString(Charsets.UTF_8)
            throw IOException("Peer returned HTTP ${response.statusCode()}: $details")
        }
        storage.merge(LwwMapState(stateCodec.decode(response.body())))
    }

    @Synchronized
    override fun close() {
        if (closed) return
        closed = true
        storage.use {
            if (started) server.stop(1)
        }
    }

    private fun handleSnapshot(exchange: HttpExchange) {
        exchange.use { exchange ->
            if (exchange.requestMethod != "POST") {
                sendResponse(exchange, 405, "Only POST is supported".toByteArray(Charsets.UTF_8), "text/plain")
                return
            }

            val incoming = try {
                exchange.requestBody.use { body ->
                    LwwMapState(stateCodec.decode(body.readBytes()))
                }
            } catch (failure: Exception) {
                sendResponse(
                    exchange,
                    400,
                    "Invalid snapshot: ${failure.message}".toByteArray(Charsets.UTF_8),
                    "text/plain",
                )
                return
            }

            val mergedSnapshot = try {
                storage.merge(incoming)
                stateCodec.encode(storage.snapshot().entries)
            } catch (_: Exception) {
                sendResponse(exchange, 500, "Could not merge snapshot".toByteArray(Charsets.UTF_8), "text/plain")
                return
            }

            sendResponse(exchange, 200, mergedSnapshot, "application/octet-stream")
        }
    }

    private fun sendResponse(exchange: HttpExchange, status: Int, body: ByteArray, contentType: String) {
        exchange.responseHeaders.set("Content-Type", contentType)
        exchange.sendResponseHeaders(status, body.size.toLong())
        exchange.responseBody.use { it.write(body) }
    }
}
