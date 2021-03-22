package com.hxm.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.List;

public class MultiSend implements Send{
    private static final Logger log = LoggerFactory.getLogger(MultiSend.class);
    private String dest;
    private long totalWritten = 0;
    private List<Send> sends;
    private Iterator<Send> sendsIterator;
    private Send current;
    private boolean doneSends = false;
    private long size = 0;

    public MultiSend(String dest, List<Send> sends) {
        this.dest = dest;
        this.sends = sends;
        this.sendsIterator = sends.iterator();
        nextSendOrDone();
        for (Send send: sends) {
            this.size += send.size();
        }
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public String destination() {
        return dest;
    }

    @Override
    public boolean completed() {
        if (doneSends) {
            if (totalWritten != size) {
                log.error("mismatch in sending bytes over socket; expected: " + size + " actual: " + totalWritten);
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public long writeTo(GatheringByteChannel channel) throws IOException {
        if (completed()) {
            throw new RuntimeException("This operation cannot be completed on a complete request.");
        }

        int totalWrittenPerCall = 0;
        boolean sendComplete = false;
        do {
            long written = current.writeTo(channel);
            totalWritten += written;
            totalWrittenPerCall += written;
            sendComplete = current.completed();
            if (sendComplete) {
                nextSendOrDone();
            }
        } while (!completed() && sendComplete);
        if (log.isTraceEnabled()) {
            log.trace("Bytes written as part of multisend call : " + totalWrittenPerCall +  "Total bytes written so far : " + totalWritten + "Expected bytes to write : " + size);
        }
        return totalWrittenPerCall;
    }

    // update current if there's a next Send, mark sends as done if there isn't
    private void nextSendOrDone() {
        if (sendsIterator.hasNext()) {
            current = sendsIterator.next();
        } else {
            doneSends = true;
        }
    }


}
