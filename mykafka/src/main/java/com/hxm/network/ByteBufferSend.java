package com.hxm.network;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public class ByteBufferSend implements Send{

    private final String destination;
    private final int size;
    protected final ByteBuffer[] buffers;
    private int remaining;
    private boolean pending = false;

    public ByteBufferSend(String destination, ByteBuffer... buffers) {
        super();
        this.destination = destination;
        this.buffers = buffers;
        for (int i = 0; i < buffers.length; i++)
            remaining += buffers[i].remaining();
        this.size = remaining;
    }

    @Override
    public String destination() {
        return destination;
    }

    @Override
    public boolean completed() {
        return remaining <= 0 && !pending;
    }

    @Override
    public long writeTo(GatheringByteChannel channel) throws IOException {
        long written = channel.write(buffers);
        if (written < 0) {
            throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
        }
        remaining -= written;
        return written;
    }

    @Override
    public long size() {
        return this.size;
    }
}
