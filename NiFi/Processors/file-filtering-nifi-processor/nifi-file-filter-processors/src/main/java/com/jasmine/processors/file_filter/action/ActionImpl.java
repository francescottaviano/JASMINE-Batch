package com.jasmine.processors.file_filter.action;

/**
 * Action abstract class
 * */
public abstract class ActionImpl<T> implements Action<T> {
    private T value;

    protected ActionImpl(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }
}
