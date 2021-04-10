package com.hxm.core.server;

import com.hxm.core.server.FetchDataInfo;

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

    public long getHw() {
        return hw;
    }
}
