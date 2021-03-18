package com.hxm.broker;

import com.hxm.producer.Crc32;
import com.hxm.producer.TopicPartition;
import lombok.val;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

public class Utils {
    public static Logger log= Logger.getLogger(Utils.class);
    public static Thread newThread(String name, Runnable runnable, boolean daemon) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(daemon);
        thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.error("Uncaught exception in thread '" + t.getName() + "':", e);
            }
        });
        return thread;
    }

    public static Thread daemonThread(String name, Runnable runnable) {
        return newThread(name, runnable, false);
    }

    public static void writeUnsignedInt(ByteBuffer buffer, int index, long value) {
        buffer.putInt(index, (int) (value & 0xffffffffL));
    }

    public static void closeQuietly(Closeable closeable, String name) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable t) {
                log.warn("Failed to close " + name, t);
            }
        }
    }

    public static <T> T notNull(T t) {
        if (t == null) {
            throw new NullPointerException();
        } else {
            return t;
        }
    }

    public static byte[] utf8(String string) {
        try {
            return string.getBytes("UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("This shouldn't happen.", e);
        }
    }

    public static String utf8(byte[] bytes) {
        try {
            return new String(bytes, "UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("This shouldn't happen.", e);
        }
    }

    public static int utf8Length(CharSequence s) {
        int count = 0;
        for (int i = 0, len = s.length(); i < len; i++) {
            char ch = s.charAt(i);
            if (ch <= 0x7F) {
                count++;
            } else if (ch <= 0x7FF) {
                count += 2;
            } else if (Character.isHighSurrogate(ch)) {
                count += 4;
                ++i;
            } else {
                count += 3;
            }
        }
        return count;
    }

    public static <T> Map<String, Map<Integer, T>> groupDataByTopic(Map<TopicPartition, T> data) {
        Map<String, Map<Integer, T>> dataByTopic = new HashMap<String, Map<Integer, T>>();
        for (Map.Entry<TopicPartition, T> entry: data.entrySet()) {
            String topic = entry.getKey().topic();
            int partition = entry.getKey().partition();
            Map<Integer, T> topicData = dataByTopic.get(topic);
            if (topicData == null) {
                topicData = new HashMap<Integer, T>();
                dataByTopic.put(topic, topicData);
            }
            topicData.put(partition, entry.getValue());
        }
        return dataByTopic;
    }

    public static long readUnsignedInt(ByteBuffer buffer, int index) {
        return buffer.getInt(index) & 0xffffffffL;
    }

    public static long crc32(byte[] bytes, int offset, int size){
        Crc32 crc = new Crc32();
        crc.update(bytes, offset, size);
        return crc.getValue();
    }

    public static void atomicMoveWithFallback(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException outer) {
            try {
                Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
                log.debug("Non-atomic move of " + source + " to " + target + " succeeded after atomic move failed due to "
                        + outer.getMessage());
            } catch (IOException inner) {
                inner.addSuppressed(outer);
                throw inner;
            }
        }
    }
}
