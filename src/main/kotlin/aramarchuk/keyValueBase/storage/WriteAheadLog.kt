package aramarchuk.keyValueBase.storage

/** One operation as stored in the log. LSNs increase in append order. */
data class WalEntry<Operation>(
    val lsn: Long,
    val operation: Operation,
)

/** Converts typed operations to/from their durable binary representation. */
interface WalOperationCodec<Operation> {
    fun encode(operation: Operation): ByteArray
    fun decode(bytes: ByteArray): Operation
}

/**
 * Append-only log used to recover the store after a process or machine failure.
 * Implementations must preserve append order. Operations should be immutable.
 * File-backed implementations use a [WalOperationCodec] for serialization.
 */
interface WriteAheadLog<Operation> : AutoCloseable {
    /**
     * Append an operation and return its log sequence number. The entry may not
     * be durable until [sync] completes.
     */
    fun append(operation: Operation): Long

    /** Make all operations appended so far durable on stable storage. */
    fun sync()

    /**
     * Read valid entries in append order, including entries not yet truncated.
     * Recovery should ignore an incomplete final entry (a torn write) and report
     * corruption in the middle of the log.
     */
    fun replay(): Sequence<WalEntry<Operation>>

    /**
     * Remove entries through [lsn]. Call only after a durable checkpoint includes
     * all operations through that sequence number. New LSNs must still increase
     * after truncation.
     */
    fun truncateThrough(lsn: Long)

    override fun close()
}
