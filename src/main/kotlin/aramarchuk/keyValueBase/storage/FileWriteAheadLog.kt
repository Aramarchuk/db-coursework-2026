package aramarchuk.keyValueBase.storage

import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption.ATOMIC_MOVE
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardOpenOption.WRITE
import java.util.zip.CRC32C

/** File-backed [WriteAheadLog]. Only one instance should write to a given path. */
class FileWriteAheadLog(path: Path) : WriteAheadLog {
    private val filePath = path.toAbsolutePath().normalize()
    private var baseLsn = 0L
    private var lastLsn = 0L
    private var validEnd = HEADER_SIZE.toLong()
    private var closed = false
    private var channel: FileChannel = openAndRecover()

    private fun openAndRecover(): FileChannel {
        Files.createDirectories(filePath.parent)
        val opened = FileChannel.open(filePath, CREATE, READ, WRITE)
        try {
            if (opened.size() == 0L) {
                writeHeader(opened, 0L)
                opened.force(true)
            } else {
                baseLsn = readHeader(opened)
                val scan = scan(opened, baseLsn)
                lastLsn = scan.lastLsn
                validEnd = scan.validEnd

                // Discard a torn final entry before allowing new appends.
                if (validEnd < opened.size()) {
                    opened.truncate(validEnd)
                    opened.force(true)
                }
            }
            return opened
        } catch (failure: Throwable) {
            opened.close()
            throw failure
        }
    }

    @Synchronized
    override fun append(operation: WalOperation): Long {
        ensureOpen()
        check(lastLsn < Long.MAX_VALUE) { "WAL LSN space exhausted" }
        val lsn = lastLsn + 1L
        val frame = encodeFrame(lsn, operation)
        val start = validEnd

        try {
            writeFully(channel, frame, start)
        } catch (failure: Throwable) {
            try {
                channel.truncate(start)
                channel.force(true)
            } catch (cleanupFailure: Throwable) {
                failure.addSuppressed(cleanupFailure)
            }
            throw failure
        }

        lastLsn = lsn
        validEnd += frame.limit()
        return lsn
    }

    @Synchronized
    override fun sync() {
        ensureOpen()
        channel.force(true)
    }

    @Synchronized
    override fun replay(): Sequence<WalEntry> {
        ensureOpen()
        return scan(channel, baseLsn).entries.asSequence().map { it.entry }
    }

    @Synchronized
    override fun truncateThrough(lsn: Long) {
        ensureOpen()
        require(lsn >= 0L) { "LSN must be non-negative" }
        if (lsn <= baseLsn) return
        require(lsn <= lastLsn) { "Cannot truncate beyond the last appended LSN ($lastLsn)" }

        val scan = scan(channel, baseLsn)
        val remainingStart = scan.entries.firstOrNull { it.entry.lsn > lsn }?.offset
            ?: scan.validEnd
        val parent = filePath.parent
        val tempPath = Files.createTempFile(parent, filePath.fileName.toString(), ".tmp")

        try {
            FileChannel.open(tempPath, READ, WRITE).use { replacement ->
                writeHeader(replacement, lsn)
                val bytesToKeep = scan.validEnd - remainingStart
                copyRange(channel, remainingStart, replacement, HEADER_SIZE.toLong(), bytesToKeep)
                replacement.force(true)
            }

            channel.force(true)
            channel.close()
            try {
                Files.move(tempPath, filePath, ATOMIC_MOVE, REPLACE_EXISTING)
            } catch (failure: AtomicMoveNotSupportedException) {
                channel = FileChannel.open(filePath, READ, WRITE)
                throw IOException("Atomic replacement is required to truncate a WAL safely", failure)
            } catch (failure: Throwable) {
                channel = FileChannel.open(filePath, READ, WRITE)
                throw failure
            }

            channel = FileChannel.open(filePath, READ, WRITE)
            baseLsn = lsn
            validEnd = channel.size()
        } finally {
            Files.deleteIfExists(tempPath)
        }
    }

    @Synchronized
    override fun close() {
        if (closed) return
        closed = true
        channel.use { channel ->
            if (channel.isOpen) channel.force(true)
        }
    }

    private fun ensureOpen() {
        check(!closed) { "WAL is closed" }
        check(channel.isOpen) { "WAL channel is closed; reopen the WAL before using it again" }
    }

    private fun encodeFrame(lsn: Long, operation: WalOperation): ByteBuffer {
        val key = operation.key
        val value = (operation as? WalOperation.Put)?.value
        val contentSize = 8L + 1L + 4L + key.size + (if (value == null) 0L else 4L + value.size)
        val frameBodySize = contentSize + CHECKSUM_SIZE
        require(frameBodySize <= MAX_FRAME_SIZE) {
            "WAL entry is too large (maximum frame size is $MAX_FRAME_SIZE bytes)"
        }

        val content = ByteBuffer.allocate(contentSize.toInt())
        content.putLong(lsn)
        when (operation) {
            is WalOperation.Put -> content.put(OP_PUT)
            is WalOperation.Delete -> content.put(OP_DELETE)
        }
        content.putInt(key.size)
        content.put(key)
        if (value != null) {
            content.putInt(value.size)
            content.put(value)
        }

        val crc = CRC32C().apply { update(content.array(), 0, content.position()) }.value.toInt()
        val frame = ByteBuffer.allocate(WAL_ENTRY_SIZE_IN_BYTES + frameBodySize.toInt())
        frame.putInt(frameBodySize.toInt())
        frame.put(content.array())
        frame.putInt(crc)
        frame.flip()
        return frame
    }

    private fun scan(source: FileChannel, fileBaseLsn: Long): ScanResult {
        val fileSize: Long = source.size()
        var offset: Long = HEADER_SIZE.toLong()
        var expectedLsn: Long = fileBaseLsn
        val entries = mutableListOf<ScannedEntry>()

        while (offset < fileSize) {
            val remaining = fileSize - offset
            if (remaining < WAL_ENTRY_SIZE_IN_BYTES) break // Torn length prefix at EOF.

            val lengthBuffer = ByteBuffer.allocate(WAL_ENTRY_SIZE_IN_BYTES)
            readFully(source, lengthBuffer, offset)
            lengthBuffer.flip()
            val frameBodySize = lengthBuffer.int
            if (frameBodySize !in MIN_FRAME_BODY_SIZE..MAX_FRAME_SIZE) {
                throw IOException("Invalid WAL frame length $frameBodySize at offset $offset")
            }
            if (remaining - WAL_ENTRY_SIZE_IN_BYTES < frameBodySize) break // Torn final frame.

            val frameBody = ByteBuffer.allocate(frameBodySize)
            readFully(source, frameBody, offset + WAL_ENTRY_SIZE_IN_BYTES)
            frameBody.flip()

            val contentSize = frameBodySize - CHECKSUM_SIZE
            val expectedCrc = ByteBuffer.wrap(frameBody.array(), contentSize, CHECKSUM_SIZE).int
            val actualCrc = CRC32C().apply { update(frameBody.array(), 0, contentSize) }.value.toInt()
            if (expectedCrc != actualCrc) {
                throw IOException("WAL checksum mismatch at offset $offset")
            }

            val content = ByteBuffer.wrap(frameBody.array(), 0, contentSize)
            val lsn = content.long
            if (expectedLsn == Long.MAX_VALUE) throw IOException("WAL LSN space exhausted")
            val nextExpectedLsn = expectedLsn + 1L
            if (lsn != nextExpectedLsn) {
                throw IOException("Unexpected WAL LSN $lsn at offset $offset; expected $nextExpectedLsn")
            }

            val operationType = content.get()
            val keyLength = readLength(content, "key", offset)
            val key = readBytes(content, keyLength, "key", offset)
            val operation = when (operationType) {
                OP_PUT -> {
                    val valueLength = readLength(content, "value", offset)
                    WalOperation.Put(key, readBytes(content, valueLength, "value", offset))
                }
                OP_DELETE -> WalOperation.Delete(key)
                else -> throw IOException("Unknown WAL operation type $operationType at offset $offset")
            }
            if (content.hasRemaining()) {
                throw IOException("Unexpected trailing bytes in WAL entry at offset $offset")
            }

            entries += ScannedEntry(WalEntry(lsn, operation), offset)
            expectedLsn = lsn
            offset += WAL_ENTRY_SIZE_IN_BYTES + frameBodySize
        }

        return ScanResult(entries, expectedLsn, offset)
    }

    private fun readLength(buffer: ByteBuffer, field: String, offset: Long): Int {
        if (buffer.remaining() < WAL_ENTRY_SIZE_IN_BYTES) {
            throw IOException("Missing $field length in WAL entry at offset $offset")
        }
        val length = buffer.int
        if (length < 0 || length > buffer.remaining()) {
            throw IOException("Invalid $field length $length in WAL entry at offset $offset")
        }
        return length
    }

    private fun readBytes(buffer: ByteBuffer, length: Int, field: String, offset: Long): ByteArray {
        if (length > buffer.remaining()) {
            throw IOException("Truncated $field in WAL entry at offset $offset")
        }
        return ByteArray(length).also { buffer.get(it) }
    }

    private fun readHeader(source: FileChannel): Long {
        if (source.size() < HEADER_SIZE) throw IOException("Truncated WAL header: $filePath")
        val header = ByteBuffer.allocate(HEADER_SIZE)
        readFully(source, header, 0L)
        header.flip()
        if (header.int != MAGIC) throw IOException("Invalid WAL magic: $filePath")
        val version = header.int
        if (version != FORMAT_VERSION) throw IOException("Unsupported WAL format version $version")
        val headerBaseLsn = header.long
        if (headerBaseLsn < 0L) throw IOException("Invalid WAL base LSN $headerBaseLsn")
        return headerBaseLsn
    }

    private fun writeHeader(target: FileChannel, headerBaseLsn: Long) {
        val header = ByteBuffer.allocate(HEADER_SIZE)
            .putInt(MAGIC)
            .putInt(FORMAT_VERSION)
            .putLong(headerBaseLsn)
        header.flip()
        writeFully(target, header, 0L)
    }

    private fun copyRange(
        source: FileChannel,
        sourceOffset: Long,
        target: FileChannel,
        targetOffset: Long,
        length: Long,
    ) {
        var copied = 0L
        while (copied < length) {
            val chunkSize = minOf(COPY_BUFFER_SIZE.toLong(), length - copied).toInt()
            val buffer = ByteBuffer.allocate(chunkSize)
            readFully(source, buffer, sourceOffset + copied)
            buffer.flip()
            writeFully(target, buffer, targetOffset + copied)
            copied += chunkSize
        }
    }

    private fun readFully(source: FileChannel, buffer: ByteBuffer, position: Long) {
        var offset = position
        while (buffer.hasRemaining()) {
            val count = source.read(buffer, offset)
            if (count < 0) throw EOFException("Unexpected end of WAL at offset $offset")
            if (count == 0) throw IOException("Could not make progress reading WAL at offset $offset")
            offset += count
        }
    }

    private fun writeFully(target: FileChannel, buffer: ByteBuffer, position: Long) {
        var offset = position
        while (buffer.hasRemaining()) {
            val count = target.write(buffer, offset)
            if (count <= 0) throw IOException("Could not make progress writing WAL at offset $offset")
            offset += count
        }
    }

    private data class ScannedEntry(val entry: WalEntry, val offset: Long)

    private data class ScanResult(
        val entries: List<ScannedEntry>,
        val lastLsn: Long,
        val validEnd: Long,
    )

    private companion object {
        const val MAGIC = 0x4B565741 // "KVWA"
        const val FORMAT_VERSION = 1
        const val HEADER_SIZE = 16
        const val WAL_ENTRY_SIZE_IN_BYTES = 4
        const val CHECKSUM_SIZE = 4
        const val MIN_FRAME_BODY_SIZE = 8 + 1 + 4 + CHECKSUM_SIZE
        const val MAX_FRAME_SIZE = 256 * 1024 * 1024
        const val COPY_BUFFER_SIZE = 64 * 1024
        const val OP_PUT: Byte = 1
        const val OP_DELETE: Byte = 2
    }
}
