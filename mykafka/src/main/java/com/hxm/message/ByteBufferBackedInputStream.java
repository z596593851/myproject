package com.hxm.message;

import lombok.val;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferBackedInputStream extends InputStream {

    private ByteBuffer buffer;
    public ByteBufferBackedInputStream(ByteBuffer buffer){
        this.buffer=buffer;
    }
    @Override
    public int read() throws IOException {
        if(buffer.hasRemaining()){
            return buffer.get()&0xFF;
        }else {
            return -1;
        }
    }
    @Override
    public int read(byte bytes[], int off, int len){
        if (buffer.hasRemaining()) {
            // Read only what's left
            int realLen = Math.min(len, buffer.remaining());
            buffer.get(bytes, off, realLen);
            return realLen;
        } else{
            return -1;
        }
    }

}
