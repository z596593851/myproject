package com.hxm.producer;

import com.sun.xml.internal.ws.encoding.soap.SerializationException;

import java.io.UnsupportedEncodingException;
import java.util.Map;

public class StringSerializer {

    private String encoding = "UTF8";

    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null)
            encodingValue = configs.get("serializer.encoding");
        if (encodingValue != null && encodingValue instanceof String)
            encoding = (String) encodingValue;
    }

    public byte[] serialize(String topic, String data) {
        try {
            if (data == null) {
                return null;
            } else {
                return data.getBytes(encoding);
            }
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when serializing string to byte[] due to unsupported encoding " + encoding);
        }
    }

    public void close() {
        // nothing to do
    }
}
