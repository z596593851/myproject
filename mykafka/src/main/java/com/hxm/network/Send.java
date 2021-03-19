package com.hxm.network;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

public interface Send {

    public String destination();

    public boolean completed();

    public long writeTo(GatheringByteChannel channel) throws IOException;

    public long size();

}
