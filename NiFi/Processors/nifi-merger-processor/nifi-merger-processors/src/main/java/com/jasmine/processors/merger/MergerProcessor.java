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
package com.jasmine.processors.merger;

import com.jasmine.processors.merger.deserializers.CityDeserializer;
import com.jasmine.processors.merger.serializers.StringSerializer;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
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
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.*;

/**
 * Merger Processor
 * Merge different files of the dataset
 * */
@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MergerProcessor extends AbstractProcessor {

    /*
    * Field separator property
    * */
    public static final PropertyDescriptor FIELD_SEPARATOR_PROPERTY = new PropertyDescriptor
            .Builder().name("FIELD_SEPARATOR")
            .displayName("field separator")
            .description("Example Property")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /*
     * City header name property
     * */
    public static final PropertyDescriptor CITY_HEADER_NAME_PROPERTY = new PropertyDescriptor
            .Builder().name("CITY_HEADER_NAME_PROPERTY")
            .displayName("city header name")
            .description("Example Property")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /*
     * Distributed cache service property
     * */
    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name("Distributed Cache Service")
            .description("The Controller Service that is used to cache flow files")
            .required(true)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();

    /*
     * Success relationship
     * */
    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("success")
            .description("Example relationship")
            .build();

    /*
     * Failure relationship
     * */
    public static final Relationship FAILURE_RELATIONSHIP = new Relationship.Builder()
            .name("failure")
            .description("Example relationship")
            .build();

    private HashMap<String, City> lacalCache;
    private ObjectMapper objectMapper;
    private String fieldSeparator;
    private String cityHeaderName;
    private CacheProvider cacheProvider;
    private int localCacheLimit;
    private double localCacheRemoveProbability;

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();

        /*
         * Adding properties to descriptors list
         * */
        descriptors.add(FIELD_SEPARATOR_PROPERTY);
        descriptors.add(CITY_HEADER_NAME_PROPERTY);
        descriptors.add(DISTRIBUTED_CACHE_SERVICE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        /*
         * Adding relationships to relationships set
         * */
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
        fieldSeparator = context.getProperty(FIELD_SEPARATOR_PROPERTY).getValue();
        cityHeaderName = context.getProperty(CITY_HEADER_NAME_PROPERTY).getValue();
        lacalCache = new HashMap<>();
        objectMapper = new ObjectMapper();

        localCacheLimit = 50;
        localCacheRemoveProbability = 0.5;

        // Build cache provider
        DistributedMapCacheClient cache = context.getProperty(DISTRIBUTED_CACHE_SERVICE)
                .asControllerService(DistributedMapCacheClient.class);

        this.cacheProvider = new CacheProvider.CacheProviderBuilder()
                .setCache(cache)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        FlowFile output = session.write(flowFile, (in, out) -> {
            CSVReader reader = new CSVReader(in, fieldSeparator, true);
            CSVWriter writer = new CSVWriter(out, fieldSeparator);

            /*
             * If has header read first line to read it and get header fields to use it in order to create output
             * */
            List<String> headerFields = reader.getHeaderFields();

            if (headerFields.size() == 0) {
                // No header fields -> return error
                reader.closeFile();
                writer.closeFile();
                exitWithFailure(flowFile, session, new Exception("CSV header malformed or not present"));
            }

            /*
             * Adding header fields
             * */
            headerFields.add(1, "timezone");
            headerFields.add(3, "country");

            // Map header
            HashMap<String, Integer> headerHashMap = new HashMap<>();
            for(int i = 0; i < headerFields.size(); i++) {
                headerHashMap.put(headerFields.get(i), i);
            }

            // Write header
            writer.writeLine(headerFields);

            // Create serializers
            StringSerializer stringSerializer = new StringSerializer();

            // Create deserializers
            CityDeserializer cityDeserializer = new CityDeserializer();

            List<String> rowValues = null;
            City city = null;
            int dateTimezone = headerHashMap.get("timezone");
            int cityPosition = headerHashMap.get(cityHeaderName);
            while ((rowValues = reader.getNextLineFields()) != null) {

                // Get city from cache
                city = getCachedCity(rowValues.get(cityPosition), lacalCache, stringSerializer, cityDeserializer);
                // if city is in cache merge date and timezone
                if (city != null) {
                    // Get UTC date time
                    rowValues.add(dateTimezone, city.getTimezone());
                    rowValues.add(2, city.getCountry());
                    writer.writeLine(rowValues);
                }
                // else go on
            }

            reader.closeFile();
            writer.closeFile();
        });

        exitWithSuccess(output, session);

    }

    private City getCachedCity(String cityName, HashMap<String, City> localCache, StringSerializer stringSerializer, CityDeserializer cityDeserializer) throws IOException {
        // Search in local cache
        City city = localCache.get(cityName);
        if (city == null) {
            // Search in redis
            city = cacheProvider.getCache().get(cityName, stringSerializer, cityDeserializer);
            if (city != null) {
                // Update local cache
                if (localCache.size() >= localCacheLimit) {
                    cleanLocalCache(localCache);
                }
                localCache.put(city.getName(), city);
            }
            // City not in cache -> return null
        }
        return city;
    }

    private void cleanLocalCache(HashMap<String,City> localCache) {
        Iterator<String> iterator = localCache.keySet().iterator();

        String key = null;
        // Iterate over all the elements
        while (iterator.hasNext()) {
            key = iterator.next();
            if (Math.random() < localCacheRemoveProbability) {
                // Remove the element
                iterator.remove();
            }
        }
    }

    private void exitWithFailure(FlowFile flowFile, ProcessSession session, Exception e) {
        // Print exception
        e.printStackTrace();
        // Exit with failure
        exit(flowFile, session, FAILURE_RELATIONSHIP);
    }


    private void exitWithSuccess(FlowFile flowFile, ProcessSession session) {
        exit(flowFile, session, SUCCESS_RELATIONSHIP);
    }

    private void exit(FlowFile flowFile, ProcessSession session, Relationship relationship) {
        session.transfer(flowFile, relationship);
    }

}
