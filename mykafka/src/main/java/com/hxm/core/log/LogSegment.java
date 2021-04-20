package com.hxm.core.log;

import com.hxm.core.server.LogOffsetMetadata;
import com.hxm.core.message.ByteBufferMessageSet;
import com.hxm.core.server.FetchDataInfo;
import com.hxm.core.message.MessageAndOffset;
import com.hxm.core.message.MessageSet;

import java.io.File;
import java.util.Iterator;

public class LogSegment {
    /**
     * 配置的segment的最大大小
     */
    public static final long segmentSize=1073741824;
    /**
     * 用于操作对应日志文件的FileMessageSet对象
     */
    private final FileMessageSet log;
    /**
     * 用于操作对应索引文件的OffsetIndex对象
     */
    private final OffsetIndex index;
    /**
     * LogSegment中第一条消息的offset值
     */
    private final long baseOffset;
    /**
     * 索引项之间间隔的最小字节数
     */
    private final int indexIntervalBytes;

    private int bytesSinceLastIndexEntry = 0;

    public LogSegment(FileMessageSet log, OffsetIndex index, long baseOffset, int indexIntervalBytes) {
        this.log = log;
        this.index = index;
        this.baseOffset = baseOffset;
        this.indexIntervalBytes = indexIntervalBytes;
    }

    public void append(long firstOffset, ByteBufferMessageSet messages){
        //firstOffset：messages中的第一条消息的offset，如果是压缩消息，则是第一条内层消息的offset
        if (messages.sizeInBytes() > 0) {
            int physicalPosition=log.sizeInBytes();
            log.append(messages);
            //检测是否在满足添加索引项的条件
            if(bytesSinceLastIndexEntry > indexIntervalBytes) {
                //添加索引
                index.append(firstOffset, physicalPosition);
                bytesSinceLastIndexEntry = 0;
            }
            //记录从上次添加索引项，在日志文件中累计加入的message集合的字节数，用于判断下次索引添加的时机
            bytesSinceLastIndexEntry += messages.sizeInBytes();
        }
    }

    public int recover(int maxMessageSize){
        index.truncate();
        index.resize(index.getMaxIndexSize());
        int validBytes=0;
        int lastIndexEntry=0;
        Iterator<MessageAndOffset> iter=log.iterator(maxMessageSize);
        while(iter.hasNext()) {
            MessageAndOffset entry=iter.next();
            entry.getMessage().ensureValid();
            if(validBytes - lastIndexEntry > indexIntervalBytes) {
                long startOffset = entry.firstOffset();
                index.append(startOffset, validBytes);
                lastIndexEntry = validBytes;
            }
            validBytes += MessageSet.entrySize(entry.getMessage());
        }
        int truncated = log.sizeInBytes() - validBytes;
        log.truncateTo(validBytes);
        index.trimToValidSize();
        return truncated;
    }

    public LogSegment(File dir, long startOffset, int indexIntervalBytes, int maxIndexSize,boolean fileAlreadyExists, int initFileSize, boolean preallocate){
        this(new FileMessageSet(Log.logFilename(dir,startOffset),fileAlreadyExists, initFileSize,preallocate),
                new OffsetIndex(Log.indexFilename(dir,startOffset),startOffset,maxIndexSize),
                startOffset,
                indexIntervalBytes);
    }

    public long nextOffset(){
        FetchDataInfo ms = read(index.getLastOffset(), null, log.sizeInBytes(),-1L,false);
        if(ms==null){
            return baseOffset;
        }else {
            if(ms.getMessageSet()==null){
                return baseOffset;
            }else {
                MessageAndOffset last = null;
                Iterator<MessageAndOffset> iterator=ms.getMessageSet().iterator();
                while (iterator.hasNext()){
                    last=iterator.next();
                }
                assert last != null;
                return last.nextOffset();
            }
        }
    }

    public long size(){
        return log.sizeInBytes();
    }

    public OffsetIndex index(){
        return index;
    }

    public FetchDataInfo read(long startOffset, Long maxOffset, int maxSize, long maxPosition, boolean minOneMessage){
        if (maxSize < 0) {
            throw new IllegalArgumentException(String.format("Invalid max size for log read (%d)",maxSize));
        }
        if(maxPosition==-1){
            maxPosition=log.sizeInBytes();
        }
        int logSize=log.sizeInBytes();
        //从log中找到对应指定offset
        OffsetPositionAndSize startOffsetAndSize=translateOffset(startOffset,0);
        if(startOffsetAndSize==null){
            return null;
        }
        OffsetPosition startPosition= startOffsetAndSize.getOffsetPosition();
        int messageSetSize=startOffsetAndSize.getSize();
        LogOffsetMetadata offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, startPosition.getPosition());
        int adjustedMaxSize=minOneMessage?Math.max(maxSize, messageSetSize):maxSize;
        if (adjustedMaxSize == 0) {
            return new FetchDataInfo(offsetMetadata, MessageSet.Empty,false);
        }
        int length=0;
        if(maxOffset==null){
            // no max offset, just read until the max position
            length=Math.min((int)(maxPosition - startPosition.getPosition()), adjustedMaxSize);
        }else {
            if (maxOffset < startOffset) {
                return new FetchDataInfo(offsetMetadata, MessageSet.Empty,false);
            }
            OffsetPositionAndSize mapping = translateOffset(maxOffset, startPosition.getPosition());
            int endPosition =mapping == null?
                    // the max offset is off the end of the log, use the end of the file
                logSize:
                mapping.getOffsetPosition().getPosition();
            length=(int)Math.min(Math.min(maxPosition, endPosition) - startPosition.getPosition(), adjustedMaxSize);
        }
        return new FetchDataInfo(offsetMetadata, log.read(startPosition.getPosition(), length), adjustedMaxSize < messageSetSize);

    }

    private OffsetPositionAndSize translateOffset(long offset, int startingFilePosition){
        //从索引中找到小于给定offset的最大offset，返回其物理地址
        int position=index.lookup(offset);
        //从指定的offset开始逐条遍历FileMessageSet中的消息，直到offset大于等于targetOffset，最后返回找到的offset
        return log.searchForOffsetWithSize(offset, Math.max(position, startingFilePosition));
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public void flush(){
        log.flush();
        index.flush();
    }

    public FileMessageSet getLog(){
        return log;
    }
}
