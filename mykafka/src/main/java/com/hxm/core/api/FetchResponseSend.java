package com.hxm.core.api;

import com.hxm.client.common.network.MultiSend;
import com.hxm.client.common.network.Send;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.List;

public class FetchResponseSend implements Send {

    private final String dest;
    private final FetchResponse fetchResponse;
    private ByteBuffer emptyBuffer;
    private int payloadSize;
    private long sent=0L;
    private boolean pending=false;
    private final ByteBuffer buffer;
    private final Send sends;

    public FetchResponseSend(String dest, FetchResponse fetchResponse) {
        this.dest = dest;
        this.fetchResponse = fetchResponse;
        this.emptyBuffer=ByteBuffer.allocate(0);
        this.payloadSize = fetchResponse.sizeInBytes();
        this.buffer = ByteBuffer.allocate(4 /* for size */ + fetchResponse.headerSizeInBytes());
        fetchResponse.writeHeaderTo(buffer);
        this.buffer.rewind();
        List<Send> sendList=new ArrayList<>();
        fetchResponse.getDataGroupedByTopic().forEach((topic,data)->{
            sendList.add(new TopicDataSend(dest,new TopicData(topic,data)));
        });
        this.sends=new MultiSend(dest,sendList);
    }

    @Override
    public String destination() {
        return dest;
    }

    @Override
    public boolean completed() {
        return sent >= size() && !pending;
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
        }
        pending = false;
        sent += written;
        return written;
    }

    @Override
    public long size() {
        return 4 /* for size byte */ + payloadSize;
    }
}
