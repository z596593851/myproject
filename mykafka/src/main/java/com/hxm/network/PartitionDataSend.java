package com.hxm.network;

import com.hxm.broker.TransportLayer;
import com.hxm.consumer.FetchResponsePartitionData;
import lombok.val;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public class PartitionDataSend implements Send{
    private final int partitionId;
    private final FetchResponsePartitionData partitionData;
    private final ByteBuffer emptyBuffer;
    private final int messageSize;
    private final ByteBuffer buffer;
    private int messagesSentSize=0;
    private boolean pending=false;


    public PartitionDataSend(int partitionId, FetchResponsePartitionData partitionData) {
        this.partitionId = partitionId;
        this.partitionData = partitionData;
        this.emptyBuffer=ByteBuffer.allocate(0);
        this.messageSize=partitionData.getMessages().sizeInBytes();
        this.buffer=ByteBuffer.allocate( 4 /** partitionId **/ + FetchResponsePartitionData.headerSize);
        buffer.putInt(partitionId);
        buffer.putLong(partitionData.getHw());
        buffer.putInt(partitionData.getMessages().sizeInBytes());
        buffer.rewind();
    }

    @Override
    public String destination() {
        return "";
    }

    @Override
    public boolean completed() {
        return !buffer.hasRemaining() && messagesSentSize >= messageSize && !pending;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) throws IOException {
        long written = 0L;
        if (buffer.hasRemaining()) {
            written += channel.write(buffer);
        }
        if (!buffer.hasRemaining()) {
            if (messagesSentSize < messageSize) {
                int bytesSent = partitionData.getMessages().writeTo(channel, messagesSentSize, messageSize - messagesSentSize);
                messagesSentSize += bytesSent;
                written += bytesSent;
            }
        }
        pending =false;
        return written;
    }

    @Override
    public long size() {
        return buffer.capacity() + messageSize;
    }
}
