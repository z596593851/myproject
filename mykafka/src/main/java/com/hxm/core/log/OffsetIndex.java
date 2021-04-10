package com.hxm.core.log;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class OffsetIndex {
    /**
     * 磁盘上的索引文件
     */
    private File file;
    /**
     * 日志文件中第一个消息的offset
     */
    private long baseOffset;

    private int maxIndexSize;
    /**
     * 每个索引8个字节，包括相对offset(4字节)，和物理地址
     */
    private int entrySize=8;
    /**
     * 用来操作索引的MappedByteBuffer
     */
    private volatile MappedByteBuffer mmap;
    /**
     * 最大索引项个数
     */
    private volatile int maxEntries;
    /**
     * 当前索引项个数
     */
    private volatile int entries;
    private long lastOffset=lastEntry().getOffset();

    public OffsetIndex(File file, long baseOffset, int maxIndexSize){
        this.file=file;
        this.baseOffset=baseOffset;
        this.maxIndexSize=maxIndexSize;
        this.entries=getMmap().position()/entrySize;
        this.mmap=getMmap();
        this.maxEntries=this.mmap.limit()/entrySize;
    }

    public void append(long offset, int position){
        if(entries==0||offset>lastOffset){
            //相对offset
            getMmap().putInt((int)(offset-baseOffset));
            getMmap().putInt(position);
            entries++;
            lastOffset=offset;
        }
    }

    public MappedByteBuffer getMmap(){
        try {
            //如果索引文件不存在，则创建新文件并返回true
            boolean newlyCreated=file.createNewFile();
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            //对于新建的索引进行扩容
            if(newlyCreated) {
                if(maxIndexSize < entrySize) {
                    throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize);
                }
                //根据maxIndexSize对索引扩容，结果是小于maxIndexSize的最大的8的倍数
                raf.setLength(roundDownToExactMultiple(maxIndexSize, entrySize));
            }
            //进行内存映射
            long len = raf.length();
            MappedByteBuffer idx = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, len);
            //将新创建的索引文件的position设置为0，从头开始写文件
            if(newlyCreated) {
                idx.position(0);
            } else {
                //对于原来就存在的索引文件，将position移动到所有索引项的结束位置
                idx.position(roundDownToExactMultiple(idx.limit(), entrySize));
            }
            return idx;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private int roundDownToExactMultiple(int number, int factor) {
        return factor * (number / factor);
    }

    public int lookup(long targetOffset){
        ByteBuffer idx=mmap.duplicate();
        int slot=indexSlotFor(idx,targetOffset);
        if(slot==-1){
            return 0;
        }else {
            return physical(idx,slot);
        }
    }

    public int indexSlotFor(ByteBuffer idx, long target){
        if(entries == 0) {
            return -1;
        }
        if(target<baseOffset){
            return -1;
        }
        int lo=0;
        int hi=entries-1;
        while(lo<hi){
            int mid=(int)Math.ceil(hi/2.0+lo/2.0);
            long indexKey=baseOffset+relativeOffset(idx,mid);
            if(target>indexKey){
                lo=mid;
            }else if(target<mid){
                hi=mid-1;
            }else {
                return mid;
            }
        }
        return lo;
    }

    // 索引项offset
    private int relativeOffset(ByteBuffer buffer, int n){
        return buffer.getInt(n * entrySize);
    }

    // 索引项物理地址
    private int physical(ByteBuffer buffer, int n){
        return buffer.getInt(n * entrySize + 4);
    }

    public OffsetPosition lastEntry(){
        if(entries==0){
            return new OffsetPosition(baseOffset, 0);
        }else {
            return new OffsetPosition(baseOffset + relativeOffset(mmap, entries-1), physical(mmap, entries-1));
        }
    }

    public long getLastOffset(){
        return lastOffset;
    }

    public boolean isFull(){
        return entries>maxEntries;
    }

    public void flush(){
        mmap.force();
    }

    public void sanityCheck(){
        assert(entries == 0 || lastOffset > baseOffset);
        long len=file.length();
        assert(len % entrySize == 0);
    }

    public void truncate(){
        truncateToEntries(0);
    }

    private void truncateToEntries(int entries){
        this.entries=entries;
        mmap.position(this.entries*entrySize);
        lastOffset=lastEntry().getOffset();
    }

    public void resize(int newSize){
        RandomAccessFile raf = null;
        try {
            raf=new RandomAccessFile(file,"rw");
            int roundedNewSize=roundDownToExactMultiple(newSize,entrySize);
            int position=mmap.position();
            raf.setLength(roundedNewSize);
            mmap=raf.getChannel().map(FileChannel.MapMode.READ_WRITE,0,roundedNewSize);
            maxEntries=mmap.limit()/entrySize;
            mmap.position(position);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                raf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public int getMaxIndexSize(){
        return this.maxIndexSize;
    }

    public void trimToValidSize(){
        resize(entrySize*entries);
    }

}
