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
package com.jasmine.processors.reformat;

import org.apache.nifi.components.AllowableValue;
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
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.util.*;
/**
 * Reformat CSV processor
 * It transform input files
 * */
@Tags({"refactor", "custom", "csv", "jasmine"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ReformatCSVProcessor extends AbstractProcessor {

    public static final PropertyDescriptor FIELD_SEPARATOR_PROP = new PropertyDescriptor
            .Builder().name("FIELD_SEPARATOR_PROP")
            .displayName("Field separator")
            .description("CSV field separator")
            .defaultValue(",")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor HAS_HEADER_PROP = new PropertyDescriptor
            .Builder().name("HAS_HEADER_PROP")
            .displayName("Has header")
            .description("Indicate if csv first line is file header")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor HEADER_PROP = new PropertyDescriptor
            .Builder().name("HEADER_PROP")
            .displayName("Header string")
            .description("Write here CSV header fields separated by field separator character")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("success")
            .description("Success Relationship")
            .build();

    public static final Relationship FAILURE_RELATIONSHIP = new Relationship.Builder()
            .name("failure")
            .description("Failure Relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(FIELD_SEPARATOR_PROP);
        descriptors.add(HAS_HEADER_PROP);
        descriptors.add(HEADER_PROP);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS_RELATIONSHIP);
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

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        // get attributes
        String fieldSeparator = context.getProperty(FIELD_SEPARATOR_PROP).getValue();
        boolean hasHeader = context.getProperty(HAS_HEADER_PROP).asBoolean();
        String header = context.getProperty(HEADER_PROP).getValue();

        FlowFile output = session.write(flowFile, (in, out) -> {

            try {
                //create csv reader
                CSVReader reader = new CSVReader(in, fieldSeparator, hasHeader);

                if (!hasHeader) {
                    //no header -> return error
                    exitWithFailure(flowFile, session, new Exception("No CSV header"));
                }

                //if has header read first line to read it and get header fields to use it in order to create output
                final List<String> headerFields = reader.getHeaderFields();


                if (headerFields.size() == 0) {
                    // ne header fields -> return error
                    exitWithFailure(flowFile, session, new Exception("CSV header malformed or not present"));
                }

                //create csv output
                CSVWriter writer = new CSVWriter(out, fieldSeparator);

                // write header
                writer.writeLine(new ArrayList<>(Arrays.asList(header.split(fieldSeparator))));

                List<String> rowValues = null;
                while ((rowValues = reader.getNextLineFields()) != null) {
                    //get UTC date time
                    String dateTime = rowValues.get(0);

                    // for each value get city and write new line
                    // (datetime, city, value)
                    for (int i = 1; i < rowValues.size(); i++) {
                        writer.writeLine(new ArrayList<>(Arrays.asList(dateTime, headerFields.get(i), rowValues.get(i))));
                    }
                }

                //close input and output stream, this will close also input and output stream
                reader.closeFile();
                writer.closeFile();

            } catch (IOException e) {
                //failure
                exitWithFailure(flowFile, session, e);
            }

        });

        //success
        exitWithSuccess(output, session);
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
