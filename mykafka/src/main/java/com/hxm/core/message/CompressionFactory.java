package com.hxm.core.message;

import com.hxm.client.common.record.CompressionType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionFactory {
    public static OutputStream apply(CompressionType compressionType, Byte messageVersion, OutputStream stream) throws IOException {
        switch (compressionType){
            case GZIP:
                return new GZIPOutputStream(stream);
            default:
                throw new RuntimeException("unknow codec");
        }
    }

    public static InputStream apply(CompressionType compressionType, Byte messageVersion, InputStream stream) throws IOException {
        switch (compressionType){
            case GZIP:
                return new GZIPInputStream(stream);
            default:
                throw new RuntimeException("unknow codec");
        }
    }
}
