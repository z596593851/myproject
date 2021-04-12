package com.hxm.client.common.protocol;

import java.nio.ByteBuffer;

/**
 * Represents a type for an array of a particular type
 */
public class ArrayOf extends Type {

    private final Type type;
    private final boolean nullable;

    public ArrayOf(Type type) {
        this(type, false);
    }

    public static ArrayOf nullable(Type type) {
        return new ArrayOf(type, true);
    }

    private ArrayOf(Type type, boolean nullable) {
        this.type = type;
        this.nullable = nullable;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public void write(ByteBuffer buffer, Object o) {
        if (o == null) {
            buffer.putInt(-1);
            return;
        }

        Object[] objs = (Object[]) o;
        int size = objs.length;
        buffer.putInt(size);
        for (int i = 0; i < size; i++) {
            type.write(buffer, objs[i]);
        }
    }

    @Override
    public Object read(ByteBuffer buffer) {
        int size = buffer.getInt();
        if (size < 0 && isNullable()) {
            return null;
        } else if (size < 0) {
            throw new RuntimeException("Array size " + size + " cannot be negative");
        }

        if (size > buffer.remaining()) {
            throw new RuntimeException("Error reading array of size " + size + ", only " + buffer.remaining() + " bytes available");
        }
        Object[] objs = new Object[size];
        for (int i = 0; i < size; i++) {
            objs[i] = type.read(buffer);
        }
        return objs;
    }

    @Override
    public int sizeOf(Object o) {
        int size = 4;
        if (o == null) {
            return size;
        }

        Object[] objs = (Object[]) o;
        for (int i = 0; i < objs.length; i++) {
            size += type.sizeOf(objs[i]);
        }
        return size;
    }

    public Type type() {
        return type;
    }

    @Override
    public String toString() {
        return "ARRAY(" + type + ")";
    }

    @Override
    public Object[] validate(Object item) {
        try {
            if (isNullable() && item == null) {
                return null;
            }

            Object[] array = (Object[]) item;
            for (int i = 0; i < array.length; i++) {
                type.validate(array[i]);
            }
            return array;
        } catch (ClassCastException e) {
            throw new RuntimeException("Not an Object[].");
        }
    }
}
