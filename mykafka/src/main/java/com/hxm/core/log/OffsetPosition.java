package com.hxm.core.log;

public class OffsetPosition {
    private long indexKey;
    private int position;

    public OffsetPosition(long indexKey, int position) {
        this.indexKey = indexKey;
        this.position = position;
    }

    public long getIndexKey() {
        return indexKey;
    }

    public int getPosition() {
        return position;
    }

    public long getOffset(){
        return indexKey;
    }
}
