package com.hxm.client.common.record;

import com.hxm.client.common.utils.Utils;
import com.hxm.client.common.utils.Crc32;

import java.nio.ByteBuffer;

public class Record {

    public static final int CRC_OFFSET = 0;
    public static final int CRC_LENGTH = 4;
    public static final int MAGIC_OFFSET = CRC_OFFSET + CRC_LENGTH;
    public static final int MAGIC_LENGTH = 1;
    public static final int ATTRIBUTES_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    public static final int ATTRIBUTE_LENGTH = 1;
    public static final int TIMESTAMP_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    public static final int TIMESTAMP_LENGTH = 8;
    public static final int KEY_SIZE_OFFSET_V0 = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
    public static final int KEY_SIZE_OFFSET_V1 = TIMESTAMP_OFFSET + TIMESTAMP_LENGTH;
    public static final int KEY_SIZE_LENGTH = 4;
    public static final int VALUE_SIZE_LENGTH = 4;
    public static final int COMPRESSION_CODEC_MASK = 0x07;
    /**
     * The size for the record header
     */
    public static final int HEADER_SIZE = CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTE_LENGTH;
    public static final int SIZE_LENGTH = 4;
    public static final int OFFSET_LENGTH = 8;
    public static final int LOG_OVERHEAD = SIZE_LENGTH + OFFSET_LENGTH;
    public static final int RECORD_OVERHEAD = HEADER_SIZE + TIMESTAMP_LENGTH + KEY_SIZE_LENGTH + VALUE_SIZE_LENGTH;
    public static final int KEY_OFFSET_V0 = KEY_SIZE_OFFSET_V0 + KEY_SIZE_LENGTH;
    public static final int KEY_OFFSET_V1 = KEY_SIZE_OFFSET_V1 + KEY_SIZE_LENGTH;
    public static final byte MAGIC_VALUE_V0 = 0;
    public static final byte MAGIC_VALUE_V1 = 1;
    public static final byte CURRENT_MAGIC_VALUE = MAGIC_VALUE_V1;
    public static final long NO_TIMESTAMP = -1L;
    public static final byte TIMESTAMP_TYPE_MASK = 0x08;
    public static final int TIMESTAMP_TYPE_ATTRIBUTE_OFFSET = 3;

    private final ByteBuffer buffer;
    private final Long wrapperRecordTimestamp;
    private final TimestampType wrapperRecordTimestampType;

    public Record(ByteBuffer buffer){
        this.buffer=buffer;
        this.wrapperRecordTimestamp=null;
        this.wrapperRecordTimestampType=null;
    }

    Record(ByteBuffer buffer, Long wrapperRecordTimestamp, TimestampType wrapperRecordTimestampType) {
        this.buffer = buffer;
        this.wrapperRecordTimestamp = wrapperRecordTimestamp;
        this.wrapperRecordTimestampType = wrapperRecordTimestampType;
    }

    public int size() {
        return buffer.limit();
    }

    public ByteBuffer buffer() {
        return this.buffer;
    }

    public ByteBuffer value() {
        return sliceDelimited(valueSizeOffset());
    }

    public static int recordSize(byte[] key, byte[] value) {
        return recordSize(key == null ? 0 : key.length, value == null ? 0 : value.length);
    }

    public static int recordSize(int keySize, int valueSize) {
        return CRC_LENGTH + MAGIC_LENGTH + ATTRIBUTE_LENGTH + TIMESTAMP_LENGTH + KEY_SIZE_LENGTH + keySize + VALUE_SIZE_LENGTH + valueSize;
    }

    public CompressionType compressionType() {
        return CompressionType.forId(buffer.get(ATTRIBUTES_OFFSET) & COMPRESSION_CODEC_MASK);
    }

    private int valueSizeOffset() {
        if (magic() == MAGIC_VALUE_V0) {
            return KEY_OFFSET_V0 + Math.max(0, keySize());
        } else {
            return KEY_OFFSET_V1 + Math.max(0, keySize());
        }
    }

    /**
     * A ByteBuffer containing the message key
     */
    public ByteBuffer key() {
        if (magic() == MAGIC_VALUE_V0) {
            return sliceDelimited(KEY_SIZE_OFFSET_V0);
        } else {
            return sliceDelimited(KEY_SIZE_OFFSET_V1);
        }
    }

    public int keySize() {
        if (magic() == MAGIC_VALUE_V0) {
            return buffer.getInt(KEY_SIZE_OFFSET_V0);
        } else {
            return buffer.getInt(KEY_SIZE_OFFSET_V1);
        }
    }

    public byte magic() {
        return buffer.get(MAGIC_OFFSET);
    }

    private ByteBuffer sliceDelimited(int start) {
        int size = buffer.getInt(start);
        if (size < 0) {
            return null;
        } else {
            ByteBuffer b = buffer.duplicate();
            b.position(start + 4);
            b = b.slice();
            b.limit(size);
            b.rewind();
            return b;
        }
    }

    public static void write(ByteBuffer buffer, long timestamp, byte[] key, byte[] value, CompressionType type, int valueOffset, int valueSize) {
        // construct the compressor with compression type none since this function will not do any
        //compression according to the input type, it will just write the record's payload as is
        Compressor compressor = new Compressor(buffer, CompressionType.NONE);
        try {
            compressor.putRecord(timestamp, key, value, type, valueOffset, valueSize);
        } finally {
            compressor.close();
        }
    }

    public static void write(Compressor compressor, long crc, byte attributes, long timestamp, byte[] key, byte[] value, int valueOffset, int valueSize) {
        // write crc
        compressor.putInt((int) (crc & 0xffffffffL));
        // write magic value
        compressor.putByte(CURRENT_MAGIC_VALUE);
        // write attributes
        compressor.putByte(attributes);
        // write timestamp
        compressor.putLong(timestamp);
        // write the key
        if (key == null) {
            compressor.putInt(-1);
        } else {
            compressor.putInt(key.length);
            compressor.put(key, 0, key.length);
        }
        // write the value
        if (value == null) {
            compressor.putInt(-1);
        } else {
            int size = valueSize >= 0 ? valueSize : (value.length - valueOffset);
            compressor.putInt(size);
            compressor.put(value, valueOffset, size);
        }
    }

    public static long computeChecksum(ByteBuffer buffer, int position, int size) {
        Crc32 crc = new Crc32();
        crc.update(buffer.array(), buffer.arrayOffset() + position, size);
        return crc.getValue();
    }

    public static long computeChecksum(long timestamp, byte[] key, byte[] value, CompressionType type, int valueOffset, int valueSize) {
        Crc32 crc = new Crc32();
        crc.update(CURRENT_MAGIC_VALUE);
        byte attributes = 0;
        if (type.id > 0) {
            attributes = (byte) (attributes | (COMPRESSION_CODEC_MASK & type.id));
        }
        crc.update(attributes);
        crc.updateLong(timestamp);
        // update for the key
        if (key == null) {
            crc.updateInt(-1);
        } else {
            crc.updateInt(key.length);
            crc.update(key, 0, key.length);
        }
        // update for the value
        if (value == null) {
            crc.updateInt(-1);
        } else {
            int size = valueSize >= 0 ? valueSize : (value.length - valueOffset);
            crc.updateInt(size);
            crc.update(value, valueOffset, size);
        }
        return crc.getValue();
    }

    public static byte computeAttributes(CompressionType type) {
        byte attributes = 0;
        if (type.id > 0) {
            attributes = (byte) (attributes | (COMPRESSION_CODEC_MASK & type.id));
        }
        return attributes;
    }

    public long timestamp() {
        if (magic() == MAGIC_VALUE_V0) {
            return NO_TIMESTAMP;
        } else {
            // case 2
            if (wrapperRecordTimestampType == TimestampType.LOG_APPEND_TIME && wrapperRecordTimestamp != null) {
                return wrapperRecordTimestamp;
            }
                // Case 1, 3
            else {
                return buffer.getLong(TIMESTAMP_OFFSET);
            }
        }
    }

    public TimestampType timestampType() {
        if (magic() == 0) {
            return TimestampType.NO_TIMESTAMP_TYPE;
        } else {
            return wrapperRecordTimestampType == null ? TimestampType.forAttributes(attributes()) : wrapperRecordTimestampType;
        }
    }

    public byte attributes() {
        return buffer.get(ATTRIBUTES_OFFSET);
    }

    public long checksum() {
        return Utils.readUnsignedInt(buffer, CRC_OFFSET);
    }

    public boolean isValid() {
        return size() >= CRC_LENGTH && checksum() == computeChecksum();
    }

    public void ensureValid() {
        if (!isValid()) {
            if (size() < CRC_LENGTH) {
                throw new RuntimeException("Record is corrupt (crc could not be retrieved as the record is too "
                        + "small, size = " + size() + ")");
            } else {
                throw new RuntimeException("Record is corrupt (stored crc = " + checksum()
                        + ", computed crc = " + computeChecksum() + ")");
            }
        }
    }
    /**
     * Compute the checksum of the record from the record contents
     */
    public long computeChecksum() {
        return computeChecksum(buffer, MAGIC_OFFSET, buffer.limit() - MAGIC_OFFSET);
    }

}
