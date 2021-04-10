package com.hxm.core.message;

import com.hxm.client.common.utils.Utils;
import com.hxm.client.common.record.CompressionType;

import java.nio.ByteBuffer;

public class Message {
    private ByteBuffer buffer;

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
    public static final int MIN_MESSAGE_OVERHEAD = KEY_OFFSET_V0 + VALUE_SIZE_LENGTH;

    public static final Byte MagicValue_V0=0;
    public static final Byte MagicValue_V1= 1;
    public static final Byte CurrentMagicValue=1;

    public static final int KeySizeLength = 4;
    public static final int KeyOffset_V0 = KEY_SIZE_OFFSET_V0 + KeySizeLength;
    public static final int KeyOffset_V1 = KEY_SIZE_OFFSET_V1 + KeySizeLength;
    public static final int ValueSizeLength = 4;

    public Message(ByteBuffer buffer){
        this.buffer=buffer;
    }

    public ByteBuffer getBuffer(){
        return this.buffer;
    }


    public CompressionType compressionType() {
        return CompressionType.forId(buffer.get(ATTRIBUTES_OFFSET) & COMPRESSION_CODEC_MASK);
    }

    public byte magic() {
        return buffer.get(MAGIC_OFFSET);
    }

    public long checksum(){
        return Utils.readUnsignedInt(buffer,CRC_OFFSET);
    }

    public long computeChecksum(){
        return Utils.crc32(buffer.array(), buffer.arrayOffset() + MAGIC_OFFSET,  buffer.limit() - MAGIC_OFFSET);
    }

    public void ensureValid(){
        if(checksum()!=computeChecksum()){
            throw new RuntimeException("Message is corrupt (stored crc = "+checksum()+", computed crc = "+computeChecksum()+")");
        }
    }

    public int size(){
        return buffer.limit();
    }

    private int keySizeOffset() {
    if (magic() == MagicValue_V0) {
        return KEY_SIZE_OFFSET_V0;
    } else {
        return KEY_SIZE_OFFSET_V1;
    }
}

    private int keySize(){
        return buffer.getInt(keySizeOffset());
    }

    private int payloadSizeOffset() {
        if (magic() == MagicValue_V0) {
            return KeyOffset_V0 + Math.max(0, keySize());
        } else {
            return KeyOffset_V1 + Math.max(0, keySize());
        }
    }

    public ByteBuffer payload(){
        return sliceDelimited(payloadSizeOffset());
    }

    private ByteBuffer sliceDelimited(int start){
        int size = buffer.getInt(start);
        if(size < 0) {
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

}
