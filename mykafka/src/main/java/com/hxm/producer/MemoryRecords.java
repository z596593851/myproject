package com.hxm.producer;

import com.hxm.broker.Utils;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Iterator;

/**
 * A {@link Records} implementation backed by a ByteBuffer.
 */
public class MemoryRecords implements Records {

    private final static int WRITE_LIMIT_FOR_READABLE_ONLY = -1;

    // the compressor used for appends-only
    private final Compressor compressor;

    // the write limit for writable buffer, which may be smaller than the buffer capacity
    private final int writeLimit;

    // the capacity of the initial buffer, which is only used for de-allocation of writable records
    private final int initialCapacity;

    // the underlying buffer used for read; while the records are still writable it is null
    private ByteBuffer buffer;

    // indicate if the memory records is writable or not (i.e. used for appends or read-only)
    private boolean writable;

    // Construct a writable memory records
    private MemoryRecords(ByteBuffer buffer, CompressionType type, boolean writable, int writeLimit) {
        this.writable = writable;
        this.writeLimit = writeLimit;
        this.initialCapacity = buffer.capacity();
        if (this.writable) {
            this.buffer = null;
            this.compressor = new Compressor(buffer, type);
        } else {
            this.buffer = buffer;
            this.compressor = null;
        }
    }

    public static MemoryRecords emptyRecords(ByteBuffer buffer, CompressionType type, int writeLimit) {
        return new MemoryRecords(buffer, type, true, writeLimit);
    }

    public static MemoryRecords emptyRecords(ByteBuffer buffer, CompressionType type) {
        // use the buffer capacity as the default write limit
        return emptyRecords(buffer, type, buffer.capacity());
    }

    public static MemoryRecords readableRecords(ByteBuffer buffer) {
        return new MemoryRecords(buffer, CompressionType.NONE, false, WRITE_LIMIT_FOR_READABLE_ONLY);
    }

    /**
     * Append the given record and offset to the buffer
     */
    public void append(long offset, Record record) {
        if (!writable) {
            throw new IllegalStateException("Memory records is not writable");
        }

        int size = record.size();
        compressor.putLong(offset);
        compressor.putInt(size);
        compressor.put(record.buffer());
        compressor.recordWritten(size + Records.LOG_OVERHEAD);
        record.buffer().rewind();
    }

    /**
     * Append a new record and offset to the buffer
     * @return crc of the record
     */
    public long append(long offset, long timestamp, byte[] key, byte[] value) {
        if (!writable) {
            throw new IllegalStateException("Memory records is not writable");
        }

        int size = Record.recordSize(key, value);
        compressor.putLong(offset);
        compressor.putInt(size);
        long crc = compressor.putRecord(timestamp, key, value);
        compressor.recordWritten(size + Records.LOG_OVERHEAD);
        return crc;
    }

    public boolean hasRoomFor(byte[] key, byte[] value) {
        if (!this.writable) {
            return false;
        }

        return this.compressor.numRecordsWritten() == 0 ?
            this.initialCapacity >= Records.LOG_OVERHEAD + Record.recordSize(key, value) :
            this.writeLimit >= this.compressor.estimatedBytesWritten() + Records.LOG_OVERHEAD + Record.recordSize(key, value);
    }

    public boolean isFull() {
        return !this.writable || this.writeLimit <= this.compressor.estimatedBytesWritten();
    }

    /**
     * Close this batch for no more appends
     */
    public void close() {
        if (writable) {
            // close the compressor to fill-in wrapper message metadata if necessary
            compressor.close();

            // flip the underlying buffer to be ready for reads
            buffer = compressor.buffer();
            buffer.flip();

            // reset the writable flag
            writable = false;
        }
    }

    /**
     * The size of this record set
     */
    @Override
    public int sizeInBytes() {
        if (writable) {
            return compressor.buffer().position();
        } else {
            return buffer.limit();
        }
    }

    /**
     * The compression rate of this record set
     */
    public double compressionRate() {
        if (compressor == null) {
            return 1.0;
        } else {
            return compressor.compressionRate();
        }
    }

    /**
     * Return the capacity of the initial buffer, for writable records
     * it may be different from the current buffer's capacity
     */
    public int initialCapacity() {
        return this.initialCapacity;
    }

    /**
     * Get the byte buffer that backs this records instance for reading
     */
    public ByteBuffer buffer() {
        if (writable) {
            throw new IllegalStateException("The memory records must not be writable any more before getting its underlying buffer");
        }

        return buffer.duplicate();
    }

    @Override
    public Iterator<LogEntry> iterator() {
        if (writable) {
            // flip on a duplicate buffer for reading
            return new RecordsIterator((ByteBuffer) this.buffer.duplicate().flip(), false);
        } else {
            // do not need to flip for non-writable buffer
            return new RecordsIterator(this.buffer.duplicate(), false);
        }
    }
    
    @Override
    public String toString() {
        Iterator<LogEntry> iter = iterator();
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        while (iter.hasNext()) {
            LogEntry entry = iter.next();
            builder.append('(');
            builder.append("offset=");
            builder.append(entry.offset());
            builder.append(",");
            builder.append("record=");
            builder.append(entry.record());
            builder.append(")");
        }
        builder.append(']');
        return builder.toString();
    }

    /** Visible for testing */
    public boolean isWritable() {
        return writable;
    }

    public static class RecordsIterator extends AbstractIterator<LogEntry> {
        private final ByteBuffer buffer;
        private final DataInputStream stream;
        private final CompressionType type;
        //标识当前迭代器是深层还是浅层（压缩或未压缩）
        private final boolean shallow;
        //迭代压缩消息的Inner Iterator
        private RecordsIterator innerIter;
        //内层迭代器需要迭代的压缩消息集合，外层迭代器此字段为空
        private final ArrayDeque<LogEntry> logEntries;
        //内层迭代器记录压缩消息中第一个消息的offset，并根据此字段计算每个消息的offset
        //外层迭代器此字段始终为-1
        private final long absoluteBaseOffset;

        //公有构造器，用来创建外层迭代器
        public RecordsIterator(ByteBuffer buffer, boolean shallow) {
            this.type = CompressionType.NONE;
            this.buffer = buffer;
            this.shallow = shallow;
            //输入流
            this.stream = new DataInputStream(new ByteBufferInputStream(buffer));
            this.logEntries = null;
            this.absoluteBaseOffset = -1;
        }

        //私有构造器，用来创建内层迭代器
        // Private constructor for inner iterator.
        private RecordsIterator(LogEntry entry) {
            this.type = entry.record().compressionType();
            this.buffer = entry.record().value();
            this.shallow = true;
            this.stream = Compressor.wrapForInput(new ByteBufferInputStream(this.buffer), type, entry.record().magic());
            //外层消息的offset
            long wrapperRecordOffset = entry.offset();

            long wrapperRecordTimestamp = entry.record().timestamp();
            this.logEntries = new ArrayDeque<>();
            // If relative offset is used, we need to decompress the entire message first to compute
            // the absolute offset. For simplicity and because it's a format that is on its way out, we
            // do the same for message format version 0
            try {
                //将内层消息全部解压出来添加到logEntries中
                while (true) {
                    try {
                        //对于内层消息，读取并解压缩消息
                        //对于外层消息或非压缩消息，仅仅是读取消息
                        LogEntry logEntry = getNextEntryFromStream();
                        if (entry.record().magic() > Record.MAGIC_VALUE_V0) {
                            Record recordWithTimestamp = new Record(
                                    logEntry.record().buffer(),
                                    wrapperRecordTimestamp,
                                    entry.record().timestampType()
                            );
                            logEntry = new LogEntry(logEntry.offset(), recordWithTimestamp);
                        }
                        logEntries.add(logEntry);
                    } catch (EOFException e) {
                        break;
                    }
                }
                if (entry.record().magic() > Record.MAGIC_VALUE_V0) {
                    this.absoluteBaseOffset = wrapperRecordOffset - logEntries.getLast().offset();
                } else {
                    this.absoluteBaseOffset = -1;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                Utils.closeQuietly(stream, "records iterator stream");
            }
        }

        @Override
        protected LogEntry makeNext() {
            if (innerDone()) {
                try {
                    //获取消息
                    LogEntry entry = getNextEntry();
                    // No more record to return.
                    if (entry == null) {
                        return allDone();
                    }

                    // Convert offset to absolute offset if needed.
                    if (absoluteBaseOffset >= 0) {
                        long absoluteOffset = absoluteBaseOffset + entry.offset();
                        entry = new LogEntry(absoluteOffset, entry.record());
                    }

                    //根据压缩类型和shallow决定是否创建内部迭代器
                    CompressionType compression = entry.record().compressionType();
                    if (compression == CompressionType.NONE || shallow) {
                        return entry;
                    } else {
                        //创建内层迭代器
                        //每迭代一个外层消息，创建一个内层迭代器用来迭代内层消息
                        innerIter = new RecordsIterator(entry);
                        //迭代内层消息
                        return innerIter.next();
                    }
                } catch (EOFException e) {
                    return allDone();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                return innerIter.next();
            }
        }

        private LogEntry getNextEntry() throws IOException {
            if (logEntries != null) {
                return getNextEntryFromEntryList();
            } else {
                return getNextEntryFromStream();
            }
        }

        private LogEntry getNextEntryFromEntryList() {
            return logEntries.isEmpty() ? null : logEntries.remove();
        }

        private LogEntry getNextEntryFromStream() throws IOException {
            // read the offset
            long offset = stream.readLong();
            // read record size
            int size = stream.readInt();
            if (size < 0) {
                throw new IllegalStateException("Record with size " + size);
            }
            // read the record, if compression is used we cannot depend on size
            // and hence has to do extra copy
            ByteBuffer rec;
            if (type == CompressionType.NONE) {
                //未压缩消息的处理
                rec = buffer.slice();
                int newPos = buffer.position() + size;
                if (newPos > buffer.limit()) {
                    return null;
                }
                buffer.position(newPos);
                rec.limit(size);
            } else {
                //压缩消息的处理
                byte[] recordBuffer = new byte[size];
                //从stream中读数据，此过程会解压消息
                stream.readFully(recordBuffer, 0, size);
                rec = ByteBuffer.wrap(recordBuffer);
            }
            return new LogEntry(offset, new Record(rec));
        }

        private boolean innerDone() {
            return innerIter == null || !innerIter.hasNext();
        }
    }
}
