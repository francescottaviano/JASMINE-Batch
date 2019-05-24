package com.jasmine.processors.merger;

import java.io.Serializable;

/**
 * City class modeling city entity
 */
public class City implements Serializable {

    private String name;
    private double lat;
    private double lon;
    private String country;
    private String timezone;

    public City() {
    }

    public City(String name, double lat, double lon, String country, String timezone) {
        this.name = name;
        this.lat = lat;
        this.lon = lon;
        this.country = country;
        this.timezone = timezone;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }
}
