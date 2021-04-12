package com.hxm.core.log;

import com.hxm.client.common.network.TransportLayer;
import com.hxm.core.message.ByteBufferMessageSet;
import com.hxm.core.message.Message;
import com.hxm.core.message.MessageAndOffset;
import com.hxm.core.message.MessageSet;
import com.hxm.client.common.utils.AbstractIterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;

public class FileMessageSet extends MessageSet {
    private File file;
    private FileChannel channel;
    private int start;
    private int end;
    private boolean isSlice;
    private int size;

    public FileMessageSet(File file, FileChannel channel, int start, int end, boolean isSlice) {
        this.file = file;
        this.channel=channel;
        this.start = start;
        this.end = end;
        this.isSlice = isSlice;
        if(isSlice){
            this.size=end-start;
        }else {
            try {
                this.size = Math.min((int)channel.size(),end)-start;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public FileMessageSet(File file, boolean fileAlreadyExists, int initFileSize, boolean preallocate){
        this(
                file,
                FileMessageSet.openChannel(file, true, fileAlreadyExists, initFileSize, preallocate),
                0,
                !fileAlreadyExists && preallocate?0:Integer.MAX_VALUE,
                false);
    }

    @Override
    public int sizeInBytes() {
        return this.size;
    }

    @Override
    public int writeTo(GatheringByteChannel destChannel, long writePosition, int size) {
        int newSize =0;
        try {
            newSize=Math.min((int)channel.size(), end) - start;
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (newSize < this.size) {
            throw new RuntimeException(String.format("Size of FileMessageSet %s has been truncated during write: old size %d, new size %d",
                    file.getAbsolutePath(), this.size, newSize));
        }
        long position = start + writePosition;
        int count = Math.min(size, sizeInBytes());
        TransportLayer transportLayer=(TransportLayer)destChannel;
        int bytesTransferred = 0;
        try {
            bytesTransferred = (int)transportLayer.transferFrom(channel, position, count);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bytesTransferred;
    }

    //查找指定消息：
    //从指定的startingPosition开始逐条遍历FileMessageSet中的消息，并将每个消息的offset与targetOffset比较，
    //直到offset大于等于targetOffset，最后返回找到的offset
    public OffsetPositionAndSize searchForOffsetWithSize(long targetOffset, int startingPosition){
        int position = startingPosition;
        ByteBuffer buffer = ByteBuffer.allocate(MessageSet.LogOverhead);
        //当前FileMessageSet的大小(字节)
        int size = sizeInBytes();
        //从position开始逐条消息遍历MessageSet.LogOverhead=8+4=12字节
        while(position + MessageSet.LogOverhead < size) {
            //重置byteBuffer的position指针，准备读入数据
            buffer.rewind();
            //读取LogOverhead这里会确保startingPosition位于一个消息的开头
            try {
                channel.read(buffer, position);
            } catch (IOException e) {
                e.printStackTrace();
            }
            //未读取到12个字节的LogOverhead
            if(buffer.hasRemaining()) {
                throw new IllegalStateException(String.format("Failed to read complete buffer for targetOffset %d startPosition %d in %s",targetOffset, startingPosition, file.getAbsolutePath()));
            }
            buffer.rewind();
            //获取消息的offset，8个字节
            long offset = buffer.getLong();
            //获取消息的size，4个字节
            int messageSize = buffer.getInt();
            if (messageSize < Message.MIN_MESSAGE_OVERHEAD) {
                throw new IllegalStateException("Invalid message size: " + messageSize);
            }
            if (offset >= targetOffset) {
                //将offset和对应position（物理地址）封装成OffsetPosition返回
                return new OffsetPositionAndSize(new OffsetPosition(offset, position), messageSize + MessageSet.LogOverhead);
            }
            //移动position准备读下个消息
            position += MessageSet.LogOverhead + messageSize;
        }
        return null;
    }

    @Override
    public Iterator<MessageAndOffset> iterator() {
        return iterator(Integer.MAX_VALUE);
    }

    public Iterator<MessageAndOffset> iterator(int maxMessageSize){
        return new AbstractIterator<MessageAndOffset>() {
            int location=start;
            int sizeOffsetLength=12;
            ByteBuffer sizeOffsetBuffer=ByteBuffer.allocate(sizeOffsetLength);

            @Override
            protected MessageAndOffset makeNext() {
                if(location+sizeOffsetLength>=end){
                    return allDone();
                }
                sizeOffsetBuffer.rewind();
                try {
                    channel.read(sizeOffsetBuffer,location);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if(sizeOffsetBuffer.hasRemaining()) {
                    return allDone();
                }
                sizeOffsetBuffer.rewind();
                long offset=sizeOffsetBuffer.getLong();
                int size=sizeOffsetBuffer.getInt();
                if(size < Message.MIN_MESSAGE_OVERHEAD || location + sizeOffsetLength + size > end) {
                    return allDone();
                }
                if(size > maxMessageSize) {
                    throw new RuntimeException(String.format("Message size exceeds the largest allowable message size (%d).",maxMessageSize));
                }

                // read the item itself
                ByteBuffer buffer = ByteBuffer.allocate(size);
                try {
                    channel.read(buffer, location + sizeOffsetLength);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if(buffer.hasRemaining()) {
                    return allDone();
                }
                buffer.rewind();

                // increment the location and return the item
                location += size + sizeOffsetLength;
                return new MessageAndOffset(offset,new Message(buffer));
            }
        };
    }

    public FileMessageSet read(int position, int size){
        if(position < 0) {
            throw new IllegalArgumentException("Invalid position: " + position);
        }
        if(size < 0) {
            throw new IllegalArgumentException("Invalid size: " + size);
        }
        int end=this.start + position + size < 0?sizeInBytes():Math.min(this.start + position + size, sizeInBytes());
        return new FileMessageSet(file, channel, this.start + position, end,true);
    }

    public static FileChannel openChannel(File file, boolean mutable, boolean fileAlreadyExists, int initFileSize, boolean preallocate){
        try {
            if (mutable) {
                if (fileAlreadyExists) {
                    return new RandomAccessFile(file, "rw").getChannel();
                } else {
                    //进行文件预分配
                    if (preallocate) {
                        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                        randomAccessFile.setLength(initFileSize);
                        return randomAccessFile.getChannel();
                    }
                    else {
                        return new RandomAccessFile(file, "rw").getChannel();
                    }
                }
            } else {
                return new FileInputStream(file).getChannel();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void flush(){
        try {
            channel.force(true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void append(ByteBufferMessageSet messages) {
        //写文件
        int written = messages.writeFullyTo(channel);
        this.size=this.size+written;
    }

    public File getFile(){
        return file;
    }

    public int truncateTo(int targetSize){
        int originalSize = sizeInBytes();
        if(targetSize > originalSize || targetSize < 0) {
            throw new RuntimeException("Attempt to truncate log segment to " + targetSize + " bytes failed, " +
                    " size of this log segment is " + originalSize + " bytes.");
        }
        try {
            if (targetSize < (int)channel.size()) {
                channel.truncate(targetSize);
                channel.position(targetSize);
                this.size=targetSize;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return originalSize - targetSize;
    }
}
