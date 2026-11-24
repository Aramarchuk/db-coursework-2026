package aramarchuk.keyValueBase.storage

/** An operation that can be replayed to reconstruct the key-value store. */
sealed interface WalOperation {
    val key: ByteArray

    data class Put(
        override val key: ByteArray,
        val value: ByteArray,
    ) : WalOperation {
        override fun equals(other: Any?): Boolean =
            this === other || (other is Put && key.contentEquals(other.key) && value.contentEquals(other.value))

        override fun hashCode(): Int = 31 * key.contentHashCode() + value.contentHashCode()
    }

    data class Delete(
        override val key: ByteArray,
    ) : WalOperation {
        override fun equals(other: Any?): Boolean =
            this === other || (other is Delete && key.contentEquals(other.key))

        override fun hashCode(): Int = key.contentHashCode()
    }
}

/** One operation as stored in the log. LSNs increase in append order. */
data class WalEntry(
    val lsn: Long,
    val operation: WalOperation,
)

/**
 * Append-only log used to recover the store after a process or machine failure.
 * Implementations must preserve append order and treat byte arrays as immutable
 * snapshots (copy them if callers may mutate them after a call).
 */
interface WriteAheadLog : AutoCloseable {
    /**
     * Append an operation and return its log sequence number. The entry may not
     * be durable until [sync] completes.
     */
    fun append(operation: WalOperation): Long

    /** Make all operations appended so far durable on stable storage. */
    fun sync()

    /**
     * Read valid entries in append order, including entries not yet truncated.
     * Recovery should ignore an incomplete final entry (a torn write) and report
     * corruption in the middle of the log.
     */
    fun replay(): Sequence<WalEntry>

    /**
     * Remove entries through [lsn]. Call only after a durable checkpoint includes
     * all operations through that sequence number. New LSNs must still increase
     * after truncation.
     */
    fun truncateThrough(lsn: Long)

    override fun close()
}
