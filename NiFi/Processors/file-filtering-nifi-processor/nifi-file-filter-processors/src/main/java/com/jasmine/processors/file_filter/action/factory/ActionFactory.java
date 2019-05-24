package com.jasmine.processors.file_filter.action.factory;

import com.jasmine.processors.file_filter.action.Action;
import com.jasmine.processors.file_filter.action.ActionType;
import com.jasmine.processors.file_filter.action.concrete.NullAction;
import com.jasmine.processors.file_filter.action.concrete.NumericAction;
import com.jasmine.processors.file_filter.action.concrete.NumericActionType;
import com.jasmine.processors.file_filter.action.concrete.RemoveValuesAction;
import com.sun.istack.internal.Nullable;

/**
 * Action factory class
 * */
public class ActionFactory {

    private ActionFactory() {}

    public static <T> Action createAction(ActionType type, @Nullable T value, @Nullable NumericActionType numericActionType) {
        switch (type) {
            case REMOVE_COLUMN:
                return createRemoveValuesAction();
            case CONTINUE:
                return createNullAction();
            case NUMERIC:
                if (value != null) {
                    return createNumericAction(value, numericActionType);
                } else {
                    return null;
                }
            default:
                return null;
        }
    }

    private static <T> Action createNumericAction(T value, NumericActionType numericActionType) {
        return new NumericAction((Double) value, numericActionType);
    }

    private static Action createNullAction() {
        return new NullAction(null);
    }

    private static Action createRemoveValuesAction() {
        return new RemoveValuesAction(null);

    }
}
