package com.jasmine.processors.file_filter;

import com.jasmine.processors.file_filter.action.Action;
import com.jasmine.processors.file_filter.action.concrete.NullAction;
import com.jasmine.processors.file_filter.action.concrete.NumericAction;
import com.jasmine.processors.file_filter.action.concrete.RemoveValuesAction;

import java.util.ArrayList;
import java.util.List;

/**
 * Filter class
 * */
public class Filter {
    private List<Regex> regexList;
    private List<Regex> regexMap;

    public Filter(List<Regex> regexList) {
        this.regexList = regexList;
        this.regexMap = new ArrayList<>();
    }

    /***
     *
     * @param toFilter string to filter
     * @param stringSeparator char[] to separate string in sections
     * @return
     */
    public String filter(String toFilter, String stringSeparator) {
        for (Regex regex: regexMap) {
            if (!toFilter.matches(regex.getRegex())) {
                toFilter = execAction(toFilter, regex.getAction(), stringSeparator, regex.getPositions());
                if (toFilter.matches(regex.getRegex())) {
                    return toFilter;
                } else {
                    return null;
                }
            }
        }
        return toFilter;
    }

    private String execAction(String toFilter, Action action, String stringSeparator, int... positions) {
        String[] splStr = toFilter.split(stringSeparator);
        for (int i = 0; i < positions.length; i++) {
            if (splStr.length > positions[i]) {
                if (action instanceof NumericAction) {
                    splStr[positions[i]] = String.valueOf(action.execAction(Double.parseDouble(splStr[positions[i]])));
                } else if (action instanceof RemoveValuesAction) {
                    splStr[positions[i]] = ((RemoveValuesAction) action).execAction(splStr[positions[i]]);
                }
            }
        }

        StringBuilder line = new StringBuilder();
        for (int j = 0; j < splStr.length - 1; j++) {
            if (!splStr[j].equals("")) {
                line.append(splStr[j]);
            }
            if (!splStr[j+1].equals("")) {
                line.append(stringSeparator);
            }
        }

        line.append(splStr[splStr.length - 1]);

        return line.toString();

    }

    public void mapRegex(String filename) {
        regexMap.clear();
        for (Regex regex : regexList) {
            if (regex.getFilename().equals(filename)) {
                regexMap.add(regex);
            }
        }
    }
}
