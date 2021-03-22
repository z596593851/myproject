package com.hxm.network;

import com.hxm.consumer.TopicData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public class TopicDataSend implements Send{

    private final String dest;
    private final TopicData topicData;
    private final ByteBuffer emptyBuffer;
    private final long sent=0L;
    private boolean pending=false;
    private final Send sends;

    public TopicDataSend(String dest, TopicData topicData) {
        this.dest = dest;
        this.topicData = topicData;
        this.emptyBuffer = ByteBuffer.allocate(0);
        //todo remember
        this.sends=new MultiSend(dest,)
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
        return 0;
    }

    @Override
    public long size() {
        return topicData.getHeaderSize()+sends.size();
    }
}
