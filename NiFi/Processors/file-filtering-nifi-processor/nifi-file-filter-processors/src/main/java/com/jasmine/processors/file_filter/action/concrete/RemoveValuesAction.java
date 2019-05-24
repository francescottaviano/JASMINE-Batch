package com.jasmine.processors.file_filter.action.concrete;

import com.jasmine.processors.file_filter.action.ActionImpl;

/**
 * Remove Values Action class
 * */
public class RemoveValuesAction extends ActionImpl<String> {

    public RemoveValuesAction(String value) {
        super(value);
    }

    @Override
    public String execAction(String value) {
        return "";
    }
}
