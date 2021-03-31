package com.hxm.message;

import com.hxm.broker.Utils;
import com.hxm.producer.*;
import lombok.val;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.*;

public class ByteBufferMessageSet extends MessageSet {
    private ByteBuffer buffer;

    @Override
    public int sizeInBytes(){
        return buffer.limit();
    }

    @Override
    public int writeTo(GatheringByteChannel channel, long offset, int maxSize) {
        if (offset > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("offset should not be larger than Int.MaxValue:"+offset);
        }
        ByteBuffer dup = buffer.duplicate();
        int position = (int)offset;
        dup.position(position);
        dup.limit(Math.min(buffer.limit(), position + maxSize));
        int result=0;
        try {
            result= channel.write(dup);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public ByteBuffer getBuffer(){
        return this.buffer;
    }

    public ByteBufferMessageSet(ByteBuffer buffer){
        this.buffer=buffer;
    }

    public Iterator<MessageAndOffset> iterator(boolean shallow) {
        return new RecordsIterator(this.buffer.duplicate(), shallow);
    }

    public void validateMessagesAndAssignOffsets(long offsetCounter){
        //遍历深层
        Iterator<MessageAndOffset> iterator=this.iterator();
        List<Message> validatedMessages=new ArrayList<>();
        while(iterator.hasNext()){
            Message message=iterator.next().getMessage();
            //一些验证步骤
            //...
            validatedMessages.add(message);
        }
        //更新外层消息的offset，将其offset更新为内部最后一条压缩消息的offset
        buffer.putLong(0,offsetCounter+validatedMessages.size()-1);
        //更新外层消息的时间戳、attribute和CRC32
        //...
        buffer.rewind();
    }

    public int writeFullyTo(GatheringByteChannel channel){
        int written = 0;
        try {
            buffer.mark();
            while (written<sizeInBytes()){
                written+=channel.write(buffer);
            }
            buffer.reset();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return written;
    }

    @Override
    public Iterator<MessageAndOffset> iterator() {
        return iterator(false);
    }

    public static Iterator<MessageAndOffset> deepIterator(MessageAndOffset wrapperMessageAndOffset, boolean ensureMatchingMagic){
        return new AbstractIterator<MessageAndOffset>() {
            Deque<MessageAndOffset> messageAndOffsets=null;
            Message wrapperMessage=wrapperMessageAndOffset.getMessage();
            long wrapperMessageOffset=wrapperMessageAndOffset.getOffset();
            long lastInnerOffset=-1L;


            {
                if(wrapperMessage.payload() == null){
                    throw new RuntimeException("Message payload is null");
                }
                InputStream inputStream= new ByteBufferBackedInputStream(wrapperMessage.payload());
                DataInputStream compressed=null;
                try {
                    compressed=new DataInputStream(CompressionFactory.apply(wrapperMessage.compressionType(), wrapperMessage.magic(), inputStream));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                Deque<MessageAndOffset> innerMessageAndOffsets = new ArrayDeque<>();
                try {
                    while (true){
                        innerMessageAndOffsets.add(readMessageFromStream(compressed));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }finally {
                    try {
                        compressed.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                messageAndOffsets= innerMessageAndOffsets;
            }

            private MessageAndOffset readMessageFromStream(DataInputStream compressed) throws IOException {
                long innerOffset = compressed.readLong();
                int recordSize = compressed.readInt();

                if (recordSize < Message.MIN_MESSAGE_OVERHEAD) {
                    throw new RuntimeException("Message found with corrupt size `$recordSize` in deep iterator");
                }

                // read the record into an intermediate record buffer (i.e. extra copy needed)
                byte[] bufferArray = new byte[recordSize];
                compressed.readFully(bufferArray, 0, recordSize);
                ByteBuffer buffer = ByteBuffer.wrap(bufferArray);

                // Override the timestamp if necessary
                Message newMessage = new Message(buffer);

                // Due to KAFKA-4298, it is possible for the inner and outer magic values to differ. We ignore
                // this and depend on the outer message in order to decide how to compute the respective offsets
                // for the inner messages
                if (ensureMatchingMagic && newMessage.magic() != wrapperMessage.magic()) {
                    throw new RuntimeException("Compressed message has magic value "+wrapperMessage.magic()+
                            "but inner message has magic value "+newMessage.magic());
                }

                lastInnerOffset = innerOffset;
                return new MessageAndOffset(innerOffset,newMessage);
            }

            @Override
            protected MessageAndOffset makeNext() {
                MessageAndOffset messageAndOffset= messageAndOffsets.pollFirst();
                if(messageAndOffset==null){
                    return allDone();
                }else {
                    if (wrapperMessage.magic() > Message.MagicValue_V0) {
                        long relativeOffset = messageAndOffset.getOffset() - lastInnerOffset;
                        long absoluteOffset = wrapperMessageOffset + relativeOffset;
                        return new MessageAndOffset(absoluteOffset,messageAndOffset.getMessage());
                    } else {
                        return messageAndOffset;
                    }
                }
            }
        };
    }

    public static class RecordsIterator extends AbstractIterator<MessageAndOffset> {
        private final ByteBuffer buffer;
        private final DataInputStream stream;
        private final CompressionType type;
        //标识当前迭代器是深层还是浅层（压缩或未压缩）
        private final boolean shallow;
        //迭代压缩消息的Inner Iterator
        private RecordsIterator innerIter;
        //内层迭代器需要迭代的压缩消息集合，外层迭代器此字段为空
        private final ArrayDeque<MessageAndOffset> logEntries;
        //内层迭代器记录压缩消息中第一个消息的offset，并根据此字段计算每个消息的offset
        //外层迭代器此字段始终为-1
        private final long absoluteBaseOffset;

        //公有构造器，用来创建外层迭代器
        public RecordsIterator(ByteBuffer buffer, boolean shallow) {
            this.type = CompressionType.NONE;
            this.buffer = buffer;
            this.shallow = shallow;
            //输入流
            this.stream = new DataInputStream(new ByteBufferInputStream(buffer));
            this.logEntries = null;
            this.absoluteBaseOffset = -1;
        }

        //私有构造器，用来创建内层迭代器
        // Private constructor for inner iterator.
        private RecordsIterator(MessageAndOffset messageAndOffset) {
            this.type = messageAndOffset.getMessage().compressionType();
            this.buffer = messageAndOffset.getMessage().getBuffer();
            this.shallow = true;
            this.stream = Compressor.wrapForInput(new ByteBufferInputStream(this.buffer), type, messageAndOffset.getMessage().magic());
            //外层消息的offset
            long wrapperRecordOffset = messageAndOffset.getOffset();

//            long wrapperRecordTimestamp = entry.record().timestamp();
            this.logEntries = new ArrayDeque<>();
            try {
                //将内层消息全部解压出来添加到logEntries中
                while (true) {
                    try {
                        //对于内层消息，读取并解压缩消息
                        //对于外层消息或非压缩消息，仅仅是读取消息
                        MessageAndOffset logEntry = getNextEntryFromStream();
                        logEntries.add(logEntry);
                    } catch (EOFException e) {
                        break;
                    }
                }
                if (messageAndOffset.getMessage().magic() > Record.MAGIC_VALUE_V0) {
                    this.absoluteBaseOffset = wrapperRecordOffset - logEntries.getLast().getOffset();
                } else {
                    this.absoluteBaseOffset = -1;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                Utils.closeQuietly(stream, "records iterator stream");
            }
        }

        @Override
        protected MessageAndOffset makeNext() {
            if (innerDone()) {
                try {
                    //获取消息
                    MessageAndOffset messageAndOffset = getNextEntry();
                    // No more record to return.
                    if (messageAndOffset == null) {
                        return allDone();
                    }
                    //根据压缩类型和shallow决定是否创建内部迭代器
                    CompressionType compression = messageAndOffset.getMessage().compressionType();
                    if (compression == CompressionType.NONE||shallow) {
                        return messageAndOffset;
                    } else {
                        //创建内层迭代器
                        //每迭代一个外层消息，创建一个内层迭代器用来迭代内层消息
                        innerIter = new RecordsIterator(messageAndOffset);
                        //迭代内层消息
                        return innerIter.next();
                    }
                } catch (EOFException e) {
                    return allDone();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                return innerIter.next();
            }
        }

        private MessageAndOffset getNextEntry() throws IOException {
            if (logEntries != null) {
                return getNextEntryFromEntryList();
            } else {
                return getNextEntryFromStream();
            }
        }

        private MessageAndOffset getNextEntryFromEntryList() {
            return logEntries.isEmpty() ? null : logEntries.remove();
        }

        private MessageAndOffset getNextEntryFromStream() throws IOException {
            long offset = stream.readLong();
            // read record size
            int size = stream.readInt();
            if (size < 0) {
                throw new IllegalStateException("Record with size " + size);
            }
            ByteBuffer rec;
            if (type == CompressionType.NONE) {
                //未压缩消息的处理
                rec = buffer.slice();
                int newPos = buffer.position() + size;
                if (newPos > buffer.limit()) {
                    return null;
                }
                buffer.position(newPos);
                rec.limit(size);
            } else {
                //压缩消息的处理
                byte[] recordBuffer = new byte[size];
                //从stream中读数据，此过程会解压消息
                stream.readFully(recordBuffer, 0, size);
                rec = ByteBuffer.wrap(recordBuffer);
            }
            return new MessageAndOffset(offset, new Message(rec));
        }

        private boolean innerDone() {
            return innerIter == null || !innerIter.hasNext();
        }

    }
}
