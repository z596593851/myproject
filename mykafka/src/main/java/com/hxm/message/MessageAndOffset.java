package com.hxm.message;

import com.hxm.producer.CompressionType;

public class MessageAndOffset {

    private Long offset;
    private Message message;

    public MessageAndOffset(Long offset, Message message) {
        this.offset = offset;
        this.message = message;
    }

    public long getOffset(){
        return offset;
    }

    public long nextOffset(){
        return offset+1;
    }

    public Message getMessage() {
        return message;
    }

    public long firstOffset(){
        if(message.compressionType()== CompressionType.NONE){
            return offset;
        }else {
            return ByteBufferMessageSet.deepIterator(this,false).next().offset;
        }

    }
}
