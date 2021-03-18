package com.hxm.broker;

import java.util.Iterator;

public abstract class IteratorTemplate<T> implements Iterator<T> {
    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T next() {
        return null;
    }
}
