package com.jasmine.processors.file_filter.action;

/**
 * Action interface
 * */
public interface Action<T> {
    T execAction(T value);
}
