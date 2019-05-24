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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

/**
 * File filter processor
 * It filters file according to regex
 * */
@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class FileFilterProcessor extends AbstractProcessor {

    public static final PropertyDescriptor REGEX_PROPERTY = new PropertyDescriptor
            .Builder().name("REGEX_PROPERTY")
            .displayName("regex")
            .description("write separated list of elements like 'filename<-regex<-action-value(-type)<-[position,position...]' " +
                    "separated by '-and-'." +
                    " Possible actions are <continue-0,numeric-value-divide,numeric-value-multiply,numeric-value-add,remove_column-0>. ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor LINE_SPLITTER_PROPERTY = new PropertyDescriptor
            .Builder().name("LINE_SPLITTER_PROPERTY")
            .displayName("line splitter")
            .description("line splitter characters")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor JUMP_LINES_PROPERTY = new PropertyDescriptor
            .Builder().name("JUMP_LINES_PROPERTY")
            .displayName("jump lines")
            .description("lines to jump from beginning")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor VERBOSE_PROPERTY = new PropertyDescriptor
            .Builder().name("VERBOSE_PROPERTY")
            .displayName("verbose")
            .description("create a flowfile with not filtered lines")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("success")
            .description("Success relationship")
            .build();

    public static final Relationship NOT_FILTERED_RELATIONSHIP = new Relationship.Builder()
            .name("not-filtered")
            .description("Not filtered relationship")
            .build();

    public static final Relationship FAILURE_RELATIONSHIP = new Relationship.Builder()
            .name("failure")
            .description("Failure relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    /**
     *  processor specific attributes
     *  */
    private Filter filter;
    private String lineSplitter;
    private Integer lineToJump;
    private Boolean verbose;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(REGEX_PROPERTY);
        descriptors.add(LINE_SPLITTER_PROPERTY);
        descriptors.add(JUMP_LINES_PROPERTY);
        descriptors.add(VERBOSE_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(NOT_FILTERED_RELATIONSHIP);
        relationships.add(FAILURE_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        List<Regex> regexList = getRegex(context);
        filter = new Filter(regexList);
        lineSplitter = context.getProperty(LINE_SPLITTER_PROPERTY).getValue();
        lineToJump = context.getProperty(JUMP_LINES_PROPERTY).asInteger();
        verbose = context.getProperty(VERBOSE_PROPERTY).asBoolean();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        FlowFile output = session.write(flowFile, (in, out) -> {
            try {
                // set filters
                filter.mapRegex(flowFile.getAttribute("filename"));

                FileReader reader = new FileReader(in);
                FileWriter writer = new FileWriter(out);

                FlowFile notFilteredOutput = null;
                FileWriter notFilteredWriter = null;
                if (verbose) {
                    notFilteredOutput = session.create(flowFile);
                    OutputStream notFilteredOut = session.write(notFilteredOutput);
                    notFilteredWriter = new FileWriter(notFilteredOut);
                }


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
                        if (verbose) {
                            if (!line.equals(filteredLine)) {
                                // line modified, log it
                                assert notFilteredWriter != null;
                                notFilteredWriter.writeLine(line);
                            }
                        }
                    } else {
                        if (verbose) {
                            assert notFilteredWriter != null;
                            notFilteredWriter.writeLine(line);
                        }
                    }
                }

                reader.closeFile();
                writer.closeFile();
                if (verbose) {
                    assert notFilteredWriter != null;
                    notFilteredWriter.closeFile();
                    exit(notFilteredOutput, session, NOT_FILTERED_RELATIONSHIP);
                }
            } catch (IOException e) {
                exitWithFailure(flowFile, session, e);
            }

        });

        exitWithSuccess(output, session);
    }

    private List<Regex> getRegex(ProcessContext context) {
        String rStr = context.getProperty(REGEX_PROPERTY).getValue();

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
    }

    private void exitWithFailure(FlowFile flowFile, ProcessSession session, Exception e) {
        //print exception
        e.printStackTrace();
        //exit with failure
        exit(flowFile, session, FAILURE_RELATIONSHIP);
    }


    private void exitWithSuccess(FlowFile flowFile, ProcessSession session) {
        exit(flowFile, session, SUCCESS_RELATIONSHIP);
    }

    private void exit(FlowFile flowFile, ProcessSession session, Relationship relationship) {
        session.transfer(flowFile, relationship);
    }
}
