package com.jasmine.processors.file_filter.action.concrete;

import com.jasmine.processors.file_filter.action.ActionImpl;

/**
 * Numeric Action class
 * */
public class NumericAction extends ActionImpl<Double> {

    private NumericActionType subtype;

    public NumericAction(Double value, NumericActionType subtype) {
        super(value);
        this.subtype = subtype;
    }

    @Override
    public Double execAction(Double value) {
        switch (this.subtype) {
            case ADD:
                return value + getValue();
            case DIVIDE:
                return value / getValue();
            case MULTIPLY:
                return value * getValue();
        }
        return value;
    }
}
