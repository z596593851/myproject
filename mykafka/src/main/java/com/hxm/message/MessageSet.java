package com.hxm.message;

import lombok.val;
import sun.invoke.empty.Empty;

import java.nio.ByteBuffer;

public abstract class MessageSet implements Iterable<MessageAndOffset> {
    public static final int MessageSizeLength = 4;
    public static final int OffsetLength = 8;
    public static final int LogOverhead = MessageSizeLength + OffsetLength;

    public static ByteBufferMessageSet Empty = new ByteBufferMessageSet(ByteBuffer.allocate(0));

    public static int entrySize(Message message){
        return LogOverhead+message.size();
    }

    public abstract int sizeInBytes();
}
