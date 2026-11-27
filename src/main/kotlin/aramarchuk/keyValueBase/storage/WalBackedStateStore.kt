package aramarchuk.keyValueBase.storage

/**
 * Generic WAL-backed state machine. The supplied transition function must
 * return a new state without mutating [currentState], so an operation can be
 * validated before it is written and made durable.
 */
class WalBackedStateStore<State, Operation>(
    private val wal: WriteAheadLog<Operation>,
    initialState: State,
    private val applyOperation: (currentState: State, operation: Operation) -> State,
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

    /** Returns the current state. Callers must treat the returned state as read-only. */
    @Synchronized
    fun state(): State {
        ensureUsable()
        return currentState
    }

    /** Applies an operation only after it has been appended and synced. */
    @Synchronized
    fun apply(operation: Operation) {
        ensureUsable()
        val nextState = applyOperation(currentState, operation)
        try {
            wal.append(operation)
            wal.sync()
        } catch (failure: Throwable) {
            // Durable state may be uncertain after an I/O failure; reopen and replay.
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
