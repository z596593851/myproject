package com.hxm.core.log;

import com.hxm.client.common.TopicPartition;
import com.hxm.client.common.utils.Time;
import com.hxm.core.common.LongRef;
import com.hxm.core.message.ByteBufferMessageSet;
import com.hxm.core.message.Message;
import com.hxm.core.message.MessageAndOffset;
import com.hxm.core.message.MessageSet;
import com.hxm.core.server.FetchDataInfo;
import com.hxm.core.server.LogOffsetMetadata;
import com.hxm.core.utils.KafkaScheduler;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

public class Log {
    private static String LogFileSuffix = ".log";
    private static String IndexFileSuffix = ".index";
    private long lastflushedTime=0;
    private final File dir;
    private final Time time;
    private volatile long recoveryPoint;
    private final ConcurrentNavigableMap<Long,LogSegment> segments;
    private volatile LogOffsetMetadata nextOffsetMetadata;
    private final KafkaScheduler scheduler;

    public Log(File dir, long recoveryPoint, KafkaScheduler scheduler, Time time){
        this.dir=dir;
        this.recoveryPoint=recoveryPoint;
        this.scheduler=scheduler;
        this.time=time;
        this.segments=new ConcurrentSkipListMap<>();
        //恢复或者创建新的.log和.index
        loadSegments();
        this.nextOffsetMetadata=new LogOffsetMetadata(getActiveSegment().nextOffset(), getActiveSegment().getBaseOffset(), (int)getActiveSegment().size());
    }

    public LogOffsetMetadata logEndOffsetMetadata(){
        return nextOffsetMetadata;
    }

    public FetchDataInfo read(long startOffset, int maxLength, Long maxOffset, boolean minOneMessage){
        LogOffsetMetadata currentNextOffsetMetadata=nextOffsetMetadata;
        //message的offset（最大offset）
        long next=currentNextOffsetMetadata.getMessageOffset();
        System.out.printf("拉取offset: %d, 最大offset: %d",startOffset, next);
        if(startOffset==next){
            return new FetchDataInfo(currentNextOffsetMetadata, MessageSet.Empty,false);
        }
        //返回与小于或等于给定键的最大键相关联的键值映射，如果没有此键，则 null
        //segments在map中是以 startOffset-segment 形式存储的，所以这样可以拿到目标offset对应的segment
        Map.Entry<Long,LogSegment> entry=segments.floorEntry(startOffset);
        if(startOffset > next || entry == null){
            throw new RuntimeException(String.format("Request for offset %d but we only have log segments in the range %d to %d.",startOffset, segments.firstKey(), next));
        }
        while(entry != null) {
            long maxPosition=0;
            if (entry == segments.lastEntry()) {
                long exposedPos = nextOffsetMetadata.getRelativePositionInSegment();
                // Check the segment again in case a new segment has just rolled out.
                if (entry != segments.lastEntry()) {
                    // New log segment has rolled out, we can read up to the file end.
                    maxPosition=(int)entry.getValue().size();
                } else {
                    maxPosition=exposedPos;
                }
            } else {
                maxPosition=entry.getValue().size();
            }
            FetchDataInfo fetchInfo = entry.getValue().read(startOffset, maxOffset, maxLength, maxPosition, minOneMessage);
            if(fetchInfo == null) {
                entry = segments.higherEntry(entry.getKey());
            } else {
                return fetchInfo;
            }
        }
        return new FetchDataInfo(nextOffsetMetadata,MessageSet.Empty,false);
    }

    private void loadSegments(){
        dir.mkdirs();
        for(File file:dir.listFiles()){
            if(file.isFile()){
                String filename=file.getName();
                if(filename.endsWith(IndexFileSuffix)){
                    File logFile=new File(file.getAbsolutePath().replace(IndexFileSuffix, LogFileSuffix));
                    if(!logFile.exists()) {
                        System.out.println(String.format("Found an orphaned index file, %s, with no corresponding log file.",file.getAbsolutePath()));
                        file.delete();
                    }
                }else if(filename.endsWith(LogFileSuffix)){
                    //此log文件对应的segment的startOffset
                    long start = Long.parseLong(filename.substring(0, filename.length() - LogFileSuffix.length()));
                    File indexFile = Log.indexFilename(dir, start);
                    boolean indexFileExists = indexFile.exists();
                    System.out.println("恢复index和log...");
                    LogSegment segment = new LogSegment(dir, start, 4096, 10485760, true,0,false);
                    if (indexFileExists) {
                        segment.index().sanityCheck();
                    } else {
                        System.out.println(String.format("Could not find index file corresponding to log file %s, rebuilding index...",segment.getLog().getFile().getAbsolutePath()));
                        segment.recover(64000);
                    }
                    segments.put(start, segment);
                }
            }
        }
        if(getlogSegments().isEmpty()){
            // no existing segments, create a new mutable segment beginning at offset 0
            segments.put(0L, new LogSegment(dir,0,4096,4096,false,0,false));
        }else {
            recoverLog();
            getActiveSegment().index().resize(64000);
        }
    }

    private void recoverLog(){
        Iterator<LogSegment> unflushed = logSegments(this.recoveryPoint, Long.MAX_VALUE).iterator();
        while(unflushed.hasNext()) {
            LogSegment curr = unflushed.next();
            System.out.println(String.format("Recovering unflushed segment %d in log %s.",curr.getBaseOffset(), getName()));
            int truncatedBytes = curr.recover(64000);
            if(truncatedBytes > 0) {
                // we had an invalid message, delete all remaining log
                System.out.println(String.format("Corruption found in segment %d of log %s, truncating to offset %d.",curr.getBaseOffset(), getName(), curr.nextOffset()));
                while(unflushed.hasNext()){
                    deleteSegment(unflushed.next());
                }
            }
        }

    }

    private void deleteSegment(LogSegment segment){
        segments.remove(segment.getBaseOffset());
    }

    public File getDir() {
        return dir;
    }

    public long getRecoveryPoint() {
        return recoveryPoint;
    }

    private LogSegment getActiveSegment(){
        return segments.lastEntry().getValue();
    }

    public LogAppendInfo append(ByteBufferMessageSet messages){
        //1、检测消息长度和CRC32校验码，返回LogAppendInfo对象
        LogAppendInfo appendInfo=analyzeAndValidateMessageSet(messages);
        //2、将未通过上述检查的部分截断
        ByteBufferMessageSet validMessages=trimInvalidBytes(messages,appendInfo);
        //拿到最后一个索引项的offset。初始值是0
        LongRef offset=new LongRef(nextOffsetMetadata.getMessageOffset());
        appendInfo.setFirstOffset(offset.getValue());
        //3、进一步验证并分配offset
        validMessages.validateMessagesAndAssignOffsets(offset);
        //记录最后一条消息的offset，并不受压缩消息的影响
        appendInfo.setLastOffset(offset.getValue()-1);
        /*假设nextOffsetMetadata.getMessageOffset()的值是0，内层消息的个数为5，经过上述步骤
        appendInfo.firstoffset=0, appendInfo.lastOffset=0+5-1=4
        buffer.offset=0+5-1=4
         */
        //5、检测是否满足创建新activeSegment的条件，如满足则创建
        LogSegment segment=maybeRoll(validMessages.sizeInBytes());
        //6、追加消息
        segment.append(appendInfo.getFirstOffset(), messages);
        //7、更新LEO
        updateLogEndOffset(appendInfo.getLastOffset()+1);
        //8、检测未刷新到磁盘的数据是否达到一定阈值，如果是则调用flush()方法刷新
        if (unflushedMessages() >= 1) {
            flush();
        }
        return appendInfo;
    }

    public static TopicPartition parseTopicPartitionName(File dir) throws IOException {
        String name=dir.getName();
        if (name.isEmpty() || !name.contains("-")) {
             throwException(dir);
        }
        int index=name.lastIndexOf("-");
        String topic=name.substring(0,index);
        String partition=name.substring(index+1);
        if(topic.length() < 1 || partition.length() < 1){
            throwException(dir);
        }
        return new TopicPartition(topic,Integer.parseInt(partition));
    }

    private static void throwException(File dir) throws IOException {
        throw new RuntimeException("Found directory " + dir.getCanonicalPath() + ", " +
                "'" + dir.getName() + "' is not in the form of topic-partition\n" +
                "If a directory does not contain Kafka topic data it should not exist in Kafka's log " +
                "directory");
    }

    public void flush(){
        flush(this.logEndOffset());
    }

    private long unflushedMessages(){
        return logEndOffset() - this.recoveryPoint;
    }

    private LogSegment maybeRoll(int messagesSize){
        //当前activeSegment的日志大小加上本次待追加的消息集合大小，超过配置的LogSegment的最大长度
        //或索引文件满了
        //创建新的segment
        if(getActiveSegment().size()>LogSegment.segmentSize-messagesSize || getActiveSegment().index().isFull()){
            return roll();
        }else {
            return getActiveSegment();
        }
    }

    public ByteBufferMessageSet trimInvalidBytes(ByteBufferMessageSet messages, LogAppendInfo info){
        int messageSetValidBytes=info.validBytesCount;
        if(messageSetValidBytes<0){
            throw new RuntimeException("Illegal length of message set " + messageSetValidBytes + " Message set cannot be appended to log. Possible causes are corrupted produce requests");
        }
        if(messageSetValidBytes == messages.sizeInBytes()) {
            return messages;
        }else {
            ByteBuffer validByteBuffer=messages.getBuffer();
            validByteBuffer.limit(messageSetValidBytes);
            return new ByteBufferMessageSet(validByteBuffer);
        }
    }

    private LogSegment roll(){
        //获取LEO
        long newOffset = logEndOffset();
        //新日志文件的文件名是LEO.log
        File logFile = logFilename(dir, newOffset);
        //新索引文件的名字是LEO.index
        File indexFile = indexFilename(dir, newOffset);
        if(logFile.exists()){
            System.out.println("Newly rolled segment file " + logFile.getName() + " already exists; deleting it first");
            logFile.delete();
        }
        if(indexFile.exists()){
            System.out.println("Newly rolled segment file " + indexFile.getName() + " already exists; deleting it first");
            indexFile.delete();
        }
        //创建新的LogSegment
        LogSegment segment = new LogSegment(dir, newOffset, 4096, 10485760, false, 0, false);
        LogSegment prev=addSegment(segment);
        if(prev != null){
            throw new RuntimeException(String.format("Trying to roll a new log segment for topic partition %s with start offset %d while it already exists.",getName(),newOffset));
        }
        //更新nextOffsetMetadata中记录的activeSegment.baseOffset和activeSegment.size，而LEO并不会改变
        updateLogEndOffset(nextOffsetMetadata.getMessageOffset());
        //异步刷盘-立刻执行
        scheduler.schedule("flush-log",()->flush(newOffset),0L,-1,TimeUnit.MICROSECONDS);
        return segment;

    }

    private void flush(long offset){
        if (offset <= this.recoveryPoint) {
            return;
        }
        //logSegments()通过对segments跳表的操作，找到recoverPoint和offset之间的LogSegment对象
        for(LogSegment segment:logSegments(this.recoveryPoint,offset)){
            //刷盘
            segment.flush();
        }
        if(offset>this.recoveryPoint){
            //后移recoverPoint
            this.recoveryPoint=offset;
            lastflushedTime=time.milliseconds();
        }
    }

    public long getLastflushedTime() {
        return lastflushedTime;
    }

    private Collection<LogSegment> logSegments(long from, long to){
        //返回小于或等于给定键的最大键，如果没有这样的键，则返回 null
        Long floor=segments.floorKey(from);
        if(floor==null){
            //返回key小于等于所给key的部分map
            return segments.headMap(to).values();
        }else {
            return segments.subMap(floor,true,to,false).values();
        }
    }


    private void updateLogEndOffset(long messageOffset) {
        nextOffsetMetadata = new LogOffsetMetadata(messageOffset, getActiveSegment().getBaseOffset(), (int)getActiveSegment().size());
    }

    private String getName(){
        return dir.getName();
    }

    private LogSegment addSegment(LogSegment segment){
        return this.segments.put(segment.getBaseOffset(),segment);
    }

    private long logEndOffset(){
        return nextOffsetMetadata.getMessageOffset();
    }

    public static File logFilename(File dir, long offset){
        return new File(dir,filenamePrefixFromOffset(offset)+LogFileSuffix);
    }

    public static File indexFilename(File dir, long offset){
        return new File(dir,filenamePrefixFromOffset(offset)+IndexFileSuffix);
    }

    private static String filenamePrefixFromOffset(long offset){
        NumberFormat nf=NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    public Collection<LogSegment> getlogSegments(){
        return segments.values();
    }

    private LogAppendInfo analyzeAndValidateMessageSet(ByteBufferMessageSet messages){
        //记录外层消息的数量
        int shallowMessageCount = 0;
        //记录通过验证的Message的字节数之和
        int validBytesCount = 0;
        //第一条消息和最后一条消息的offset（外层）
        long firstOffset= -1L;
        long lastOffset = -1L;
        //offset是否单调递增
        boolean monotonic = true;
        //压缩方式
        int sourceCodec=0;
        //遍历浅层
        Iterator<MessageAndOffset> msgIter= messages.iterator(true);
        while (msgIter.hasNext()){
            MessageAndOffset messageAndOffset=msgIter.next();
            if(firstOffset < 0) {
                firstOffset = messageAndOffset.getOffset();
            }
            if(lastOffset >= messageAndOffset.getOffset()) {
                monotonic = false;
            }
            lastOffset = messageAndOffset.getOffset();
            Message m=messageAndOffset.getMessage();
            int messageSize = MessageSet.entrySize(m);
            //检查messages的crc32校验码
            m.ensureValid();
            //增加通过校验的外层消息数
            shallowMessageCount++;
            validBytesCount += messageSize;
            sourceCodec=m.compressionType().id;
        }
        return new LogAppendInfo(firstOffset,lastOffset,sourceCodec,shallowMessageCount,validBytesCount,monotonic);
    }

    static class LogAppendInfo{
        private long firstOffset;
        private long lastOffset;
        private int sourceCodec;
        private int shallowMessageCount;
        private int validBytesCount;
        private boolean monotonic;

        public LogAppendInfo(long firstOffset, long lastOffset, int sourceCodec, int shallowMessageCount, int validBytesCount, boolean monotonic) {
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
            this.sourceCodec = sourceCodec;
            this.shallowMessageCount = shallowMessageCount;
            this.validBytesCount = validBytesCount;
            this.monotonic = monotonic;
        }

        public void setFirstOffset(long firstOffset) {
            this.firstOffset = firstOffset;
        }

        public void setLastOffset(long lastOffset) {
            this.lastOffset = lastOffset;
        }

        public long getFirstOffset() {
            return firstOffset;
        }

        public long getLastOffset() {
            return lastOffset;
        }
    }

}
