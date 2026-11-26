package aramarchuk.keyValueBase

import aramarchuk.keyValueBase.storage.Replica
import aramarchuk.keyValueBase.storage.ReplicaConfig
import java.net.URI
import java.nio.file.Path

fun main(args: Array<String>) {
    val config = try {
        parseConfig(args)
    } catch (failure: IllegalArgumentException) {
        System.err.println(failure.message)
        return
    }
    val replica = try {
        Replica(config)
    } catch (failure: Exception) {
        System.err.println("Could not create replica: ${failure.message}")
        return
    }

    replica.use { replica ->
        replica.start()
        println("Replica ${config.replicaId} listening at http://127.0.0.1:${config.port}/snapshot")
        println("Commands: put <key> <value>, get <key>, delete <key>, sync <peer-url>, exit")

        while (true) {
            print("> ")
            System.out.flush()

            val line = readlnOrNull() ?: break
            if (line.isBlank()) continue

            val parts = line.trim().split(Regex("\\s+"), limit = 3)
            when (parts[0].lowercase()) {
                "put" -> {
                    if (parts.size < 3) {
                        println("Usage: put <key> <value>")
                    } else {
                        replica.storage.put(parts[1], parts[2].toByteArray(Charsets.UTF_8))
                        println("OK")
                    }
                }

                "get" -> {
                    if (parts.size != 2) {
                        println("Usage: get <key>")
                    } else {
                        val value = replica.storage.get(parts[1])
                        println(value?.toString(Charsets.UTF_8) ?: "(not found)")
                    }
                }

                "delete" -> {
                    if (parts.size != 2) {
                        println("Usage: delete <key>")
                    } else {
                        replica.storage.delete(parts[1])
                        println("OK")
                    }
                }

                "sync" -> {
                    if (parts.size != 2) {
                        println("Usage: sync <peer-url>")
                    } else {
                        try {
                            replica.sync(URI.create(parts[1]))
                            println("Synchronized")
                        } catch (failure: Exception) {
                            println("Sync failed: ${failure.message}")
                        }
                    }
                }

                "exit", "quit" -> break
                else -> println("Unknown command: ${parts[0]}")
            }
        }
    }
}

private fun parseConfig(args: Array<String>): ReplicaConfig {
    val usage = "Usage: run --id <number> --port <number> --data-dir <path>"
    require(args.size % 2 == 0) { usage }

    val options = mutableMapOf<String, String>()
    var index = 0
    while (index < args.size) {
        val name = args[index]
        require(name in setOf("--id", "--port", "--data-dir")) { "Unknown option: $name\n$usage" }
        require(options.put(name, args[index + 1]) == null) { "Option repeated: $name\n$usage" }
        index += 2
    }

    val replicaId = options["--id"]?.toLongOrNull()?.takeIf { it >= 0L }
        ?: throw IllegalArgumentException("A non-negative --id is required\n$usage")
    val port = options["--port"]?.toIntOrNull()?.takeIf { it in 1..65535 }
        ?: throw IllegalArgumentException("A valid --port is required\n$usage")
    val dataDirectory = options["--data-dir"]?.let { Path.of(it) }
        ?: throw IllegalArgumentException("A --data-dir is required\n$usage")

    return ReplicaConfig(replicaId, port, dataDirectory)
}
