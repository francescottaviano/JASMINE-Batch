package com.jasmine.processors.merger.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jasmine.processors.merger.City;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;

import java.io.IOException;

/**
 * City Deserializer class
 */
public class CityDeserializer implements Deserializer<City> {
    private ObjectMapper objectMapper;

    public CityDeserializer() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public City deserialize(byte[] bytes) throws DeserializationException, IOException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return objectMapper.readValue(bytes, City.class);
    }
}
