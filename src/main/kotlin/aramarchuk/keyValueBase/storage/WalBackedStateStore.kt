package aramarchuk.keyValueBase.storage

/**
 * Rebuilds a state from the WAL and applies new operations after they are durable.
 * The transition must return a new state without mutating the current one.
 */
class WalBackedStateStore<S>(
    private val wal: WriteAheadLog,
    initialState: S,
    private val applyOperation: (currentState: S, operation: WalOperation) -> S,
) : AutoCloseable {
    private var currentState = initialState
    private var closed = false
    private var walFailure: Throwable? = null

    init {
        try {
            wal.replay().forEach { entry ->
                currentState = applyOperation(currentState, entry.operation)
            }
        } catch (failure: Throwable) {
            try {
                wal.close()
            } catch (closeFailure: Throwable) {
                failure.addSuppressed(closeFailure)
            }
            throw failure
        }
    }

    @Synchronized
    fun state(): S {
        ensureUsable()
        return currentState
    }

    @Synchronized
    fun apply(operation: WalOperation) {
        ensureUsable()
        val nextState = applyOperation(currentState, operation)
        try {
            wal.append(operation)
            wal.sync()
        } catch (failure: Throwable) {
            walFailure = failure
            throw failure
        }
        currentState = nextState
    }

    @Synchronized
    override fun close() {
        if (closed) return
        closed = true
        wal.close()
    }

    private fun ensureUsable() {
        check(!closed) { "Store is closed" }
        walFailure?.let { failure ->
            throw IllegalStateException("Store is unusable after a WAL failure; reopen it", failure)
        }
    }
}
