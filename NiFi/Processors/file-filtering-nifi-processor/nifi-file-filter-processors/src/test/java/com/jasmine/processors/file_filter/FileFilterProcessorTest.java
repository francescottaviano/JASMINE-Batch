/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jasmine.processors.file_filter;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;


public class FileFilterProcessorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(FileFilterProcessor.class);
    }

    @Test
    public void testProcessor() {
        testRunner.setProperty("REGEX_PROPERTY", "city_attribute.csv<-^(([^,]+,)){2}[^,]+$<-continue-0<-[]-and-weather_description.csv<-^(([^,]+,)){2}[^,]+$<-continue-0<-[]-and-pressure.csv<-^(([^,]+,)){2}(7[7-9][0-9]|[8-9][0-9]{2}|1[0-2][0-9]{2})(\\.([0-9]+))$<-continue-0<-[]-and-humidity.csv<-^(([^,]+,)){2}((100(\\.)0+)|((|[0-9]|[1-9][0-9])(\\.([0-9]+))))$<-continue-0<-[]-and-temperature.csv<-^(([^,]+,)){2}(2[2-9][0-9]|3[0-2][0-9])(\\.([0-9]+))*$<-divide-1000.0<-[2]");
        testRunner.setProperty("LINE_SPLITTER_PROPERTY", ",");
        testRunner.setProperty("JUMP_LINES_PROPERTY", "1");
        testRunner.setProperty("VERBOSE_PROPERTY","false");
        testRunner.run();
    }

    @Test
    public void testCode() throws IOException {
        /*String lineSplitter = ",";
        Integer lineToJump = 1;
        String filename = "temperature.csv";
        Filter filter = new Filter(getRegex());
        filter.mapRegex(filename);

        FileReader reader = new FileReader(new FileInputStream("/Users/simone/IdeaProjects/file-filter-processor/nifi-file-filter-processors/src/test/java/com/jasmine/processors/file_filter/data/" + filename));
        FileWriter writer = new FileWriter(new FileOutputStream("/Users/simone/IdeaProjects/file-filter-processor/nifi-file-filter-processors/src/test/java/com/jasmine/processors/file_filter/data/out/" + filename));
        FileWriter notFilteredWriter = new FileWriter(new FileOutputStream("/Users/simone/IdeaProjects/file-filter-processor/nifi-file-filter-processors/src/test/java/com/jasmine/processors/file_filter/data/out/not_filtered_" + filename));

        String line = null;
        //jump lines
        for (int k = 0; k < lineToJump; k++) {
            line = reader.getNextLine();
            writer.writeLine(line);
        }

        String filteredLine = null;
        while ((line = reader.getNextLine()) != null) {
            if ((filteredLine = filter.filter(line, lineSplitter)) != null) {
                writer.writeLine(filteredLine);
                if (!line.equals(filteredLine)) {
                    // line modified, log it
                    notFilteredWriter.writeLine(line);
                }
            } else {
                notFilteredWriter.writeLine(line);
            }
        }

        reader.closeFile();
        writer.closeFile();
        notFilteredWriter.closeFile();*/
    }

    /*private List<Regex> getRegex() {
        String rStr = "city_attributes.csv<-^(([^,]+,)){2}[^,]+$<-continue-0<-[]" +
                "-and-" +
                "city_attributes.csv<-^[^,]+$<-remove_column-0<-[1,2]" +
                "-and-" +
                "weather_description.csv<-^(([^,]+,)){2}[^,]+$<-continue-0<-[]" +
                "-and-" +
                "pressure.csv<-^(([^,]+,)){2}(7[7-9][0-9]|[8-9][0-9]{2}|1[0-2][0-9]{2})(\\.([0-9]+))$<-continue-0<-[]" +
                "-and-" +
                "humidity.csv<-^(([^,]+,)){2}((100(\\.)0+)|((|[0-9]|[1-9][0-9])(\\.([0-9]+))))$<-continue-0<-[]" +
                "-and-" +
                "temperature.csv<-^(([^,]+,)){2}(2[2-9][0-9]|3[0-2][0-9])(\\.([0-9]+))*$<-numeric-1000.0-divide<-[2]";

        String[] commands = rStr.split("-and-");

        List<Regex> regexList = new ArrayList<>();
        for (String command : commands) {
            Regex regex = parseRegex(command);
            regexList.add(regex);
        }

        return regexList;
    }

    private Regex parseRegex(String str) {
        String[] strings = str.split("<-");
        String[] positionsStr;
        if (strings[3].length() > 2) {
            String positionsSub = strings[3].substring(1, strings[3].length() - 1);
            if (positionsSub.length() > 1) {
                positionsStr = positionsSub.split(",");
            } else {
                positionsStr = new String[]{positionsSub};
            }
        } else {
            positionsStr = new String[]{};
        }
        return new Regex(strings[0], strings[1], strings[2], positionsStr);
    }*/

}
