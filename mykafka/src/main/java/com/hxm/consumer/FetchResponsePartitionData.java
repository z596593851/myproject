package com.hxm.consumer;

import com.hxm.message.ByteBufferMessageSet;
import com.hxm.message.MessageSet;
import lombok.val;

import java.nio.ByteBuffer;

public class FetchResponsePartitionData {
    private short error=(short) 7;
    private final long hw;
    private final MessageSet messages;
    private final int sizeInBytes;
    public static int headerSize =
            2 + /* error code */
            8 + /* high watermark */
            4; /* messageSetSize */

    public FetchResponsePartitionData(short error,long hw, MessageSet messages) {
        this.error=error;
        this.hw = hw;
        this.messages = messages;
        this.sizeInBytes = FetchResponsePartitionData.headerSize + messages.sizeInBytes();
    }

    public FetchResponsePartitionData(long hw, MessageSet messages) {
        this.hw = hw;
        this.messages = messages;
        this.sizeInBytes = FetchResponsePartitionData.headerSize + messages.sizeInBytes();
    }

    public int sizeInBytes(){
        return sizeInBytes;
    }

    public long getHw() {
        return hw;
    }

    public short getError() {
        return error;
    }

    public MessageSet getMessages() {
        return messages;
    }

    public static FetchResponsePartitionData readFrom(ByteBuffer buffer){
        short error = buffer.getShort();
        long hw = buffer.getLong();
        int messageSetSize = buffer.getInt();
        ByteBuffer messageSetBuffer = buffer.slice();
        messageSetBuffer.limit(messageSetSize);
        buffer.position(buffer.position() + messageSetSize);
        return new FetchResponsePartitionData(error,hw, new ByteBufferMessageSet(messageSetBuffer));
    }


}
