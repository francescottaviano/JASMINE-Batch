package com.jasmine.processors.file_filter.action.concrete;

import com.jasmine.processors.file_filter.action.ActionImpl;

/**
 * Null Action class
 * */
public class NullAction extends ActionImpl<Void> {

    public NullAction(Void value) {
        super(value);
    }

    @Override
    public Void execAction(Void value) {
        return null;
    }
}
