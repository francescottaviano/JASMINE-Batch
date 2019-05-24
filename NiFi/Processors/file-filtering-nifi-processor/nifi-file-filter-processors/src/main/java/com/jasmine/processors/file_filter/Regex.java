package com.jasmine.processors.file_filter;


import com.jasmine.processors.file_filter.action.Action;
import com.jasmine.processors.file_filter.action.ActionType;
import com.jasmine.processors.file_filter.action.concrete.NumericActionType;
import com.jasmine.processors.file_filter.action.factory.ActionFactory;

/**
 * Regex class
 * */
public class Regex {
    private String filename;
    private String regex;
    private Action action;
    private int[] positions; //line position to apply action

    public Regex(String filename, String regex, String action, String[] positions) {
        this.filename = filename;
        this.regex = regex;
        this.action = parseAction(action);
        this.positions = parsePositions(positions);
    }

    private Action parseAction(String action) {
        String[] strings = action.split("-");
        if (strings.length >= 2) {
            switch (ActionType.valueOf(strings[0].toUpperCase())) {
                case NUMERIC:
                    return ActionFactory.createAction(ActionType.NUMERIC,
                            Double.parseDouble(strings[1]), NumericActionType.valueOf(strings[2].toUpperCase()));
                case CONTINUE:
                    return ActionFactory.createAction(ActionType.CONTINUE, null, null);
                case REMOVE_COLUMN:
                    return ActionFactory.createAction(ActionType.REMOVE_COLUMN, null, null);
                default:
                    return ActionFactory.createAction(ActionType.CONTINUE, null, null);
            }
        } else {
            return ActionFactory.createAction(ActionType.CONTINUE, null, null);
        }
    }

    private int[] parsePositions(String[] textValues) {

        int[] positions = new int[textValues.length];

        for (int i = 0; i < textValues.length; i++) {
            positions[i] = Integer.parseInt(textValues[i]);
        }

        return positions ;
    }

    public String getFilename() {
        return filename;
    }

    public String getRegex() {
        return regex;
    }

    public Action getAction() {
        return action;
    }

    public int[] getPositions() {
        return positions;
    }
}
