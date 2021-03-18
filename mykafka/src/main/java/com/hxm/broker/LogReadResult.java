package com.hxm.broker;

import com.hxm.message.FetchDataInfo;

public class LogReadResult {
    private final FetchDataInfo info;
    private final long hw;
    private final int readSize;
    private final boolean isReadFromLogEnd;

    public LogReadResult(FetchDataInfo info, long hw, int readSize, boolean isReadFromLogEnd) {
        this.info = info;
        this.hw = hw;
        this.readSize = readSize;
        this.isReadFromLogEnd = isReadFromLogEnd;
    }

    public FetchDataInfo getInfo() {
        return info;
    }
}
