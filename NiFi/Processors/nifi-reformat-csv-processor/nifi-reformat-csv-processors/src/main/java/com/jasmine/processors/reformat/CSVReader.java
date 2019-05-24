package com.jasmine.processors.reformat;

import java.io.*;
import java.util.Arrays;
import java.util.List;

/**
 * CSV Reader class
 * */
public class CSVReader {
    private BufferedReader br;

    private String fieldSeparator;

    private String header;
    private boolean hasHeader;

    public CSVReader(InputStream in, String fieldSeparator, boolean hasHeader) {
        this.br = new BufferedReader(new InputStreamReader(in));
        this.fieldSeparator = fieldSeparator;
        this.hasHeader = hasHeader;
    }

    public List<String> getHeaderFields() throws IOException {
        if (this.hasHeader) {
            if (this.header == null || this.header.isEmpty()) {
                // read first line has header
                getNextLine();
            }

            return Arrays.asList(this.header.split(fieldSeparator));

        } else {
            return null;
        }
    }

    public void closeFile() throws IOException {
        this.br.close();
    }

    public List<String> getNextLineFields() throws IOException {
        String line = getNextLine();

        if (line == null) {
            return null;
        }

        return Arrays.asList(line.split(fieldSeparator));
    }

    private String getNextLine() throws IOException {
        String line = br.readLine();
        if (line == null)
            return null;

        if (this.hasHeader) {
            //save csv header
            this.header = line;
        }

        return line;
    }


    public BufferedReader getBr() {
        return br;
    }

    public String getFieldSeparator() {
        return fieldSeparator;
    }

    public String getHeader() {
        return header;
    }

    public boolean isHasHeader() {
        return hasHeader;
    }

}
