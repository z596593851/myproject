package com.hxm.log;

public class OffsetPositionAndSize {
    private OffsetPosition offsetPosition;
    private int size;

    public OffsetPositionAndSize(OffsetPosition offsetPosition, int size) {
        this.offsetPosition = offsetPosition;
        this.size = size;
    }

    public OffsetPosition getOffsetPosition() {
        return offsetPosition;
    }

    public int getSize() {
        return size;
    }
}
