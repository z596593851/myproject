package com.hxm.core.log;

import com.hxm.core.log.OffsetPosition;

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
