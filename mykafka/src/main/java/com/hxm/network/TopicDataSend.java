package com.hxm.network;

import com.hxm.broker.Utils;
import com.hxm.consumer.TopicData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.List;

public class TopicDataSend implements Send{

    private final String dest;
    private final TopicData topicData;
    private final ByteBuffer emptyBuffer;
    private long sent=0L;
    private boolean pending=false;
    private final Send sends;
    private final ByteBuffer buffer;


    public TopicDataSend(String dest, TopicData topicData) {
        this.dest = dest;
        this.topicData = topicData;
        this.emptyBuffer = ByteBuffer.allocate(0);
        this.buffer=ByteBuffer.allocate(topicData.getHeaderSize());
        Utils.writeShortString(buffer,topicData.getTopic());
        buffer.putInt(topicData.getPartitionData().size());
        buffer.rewind();
        List<Send> sendList=new ArrayList<>();
        topicData.getPartitionData().forEach(pair->sendList.add(new PartitionDataSend(pair.getKey(),pair.getValue())));
        this.sends=new MultiSend(dest,sendList);
    }

    @Override
    public String destination() {
        return this.dest;
    }

    @Override
    public boolean completed() {
        return sent>=size()&&!pending;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) throws IOException {
        if (completed()) {
            throw new RuntimeException("This operation cannot be completed on a complete request.");
        }
        long written = 0L;
        if (buffer.hasRemaining()) {
            written += channel.write(buffer);
        }
        if (!buffer.hasRemaining()) {
            if (!sends.completed()) {
                written += sends.writeTo(channel);
            }
//            if (sends.completed() && hasPendingWrites(channel)) {
//                written += channel.write(emptyBuffer);
//            }
        }

        pending = false;

        sent += written;
        return written;
    }

    @Override
    public long size() {
        return topicData.getHeaderSize()+sends.size();
    }
}
