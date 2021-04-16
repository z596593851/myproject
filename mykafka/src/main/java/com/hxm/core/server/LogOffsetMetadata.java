package com.hxm.core.server;

public class LogOffsetMetadata {
    /**
     * message的offset
     */
    private final long messageOffset;
    /**
     * activeSegment的baseOffset
     */
    private final long messsegmentBaseOffsetageOffset;
    /**
     * activeSegment的physicalPosition
     */
    private final int relativePositionInSegment;
    public static LogOffsetMetadata UnknownOffsetMetadata = new LogOffsetMetadata(-1, 0, 0);

    public LogOffsetMetadata(long messageOffset, long messsegmentBaseOffsetageOffset, int relativePositionInSegment) {
        this.messageOffset = messageOffset;
        this.messsegmentBaseOffsetageOffset = messsegmentBaseOffsetageOffset;
        this.relativePositionInSegment = relativePositionInSegment;
    }

    public long getMessageOffset() {
        return messageOffset;
    }

    public long getMesssegmentBaseOffsetageOffset() {
        return messsegmentBaseOffsetageOffset;
    }

    public int getRelativePositionInSegment() {
        return relativePositionInSegment;
    }
}
