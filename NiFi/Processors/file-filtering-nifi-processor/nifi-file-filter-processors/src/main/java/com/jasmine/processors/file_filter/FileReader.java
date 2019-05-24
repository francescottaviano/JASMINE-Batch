package com.jasmine.processors.file_filter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * File Reader class
 * */
public class FileReader {
    private BufferedReader br;

    public FileReader(InputStream in) {
        this.br = new BufferedReader(new InputStreamReader(in));
    }


    public void closeFile() throws IOException {
        this.br.close();
    }

    public String getNextLine() throws IOException {
        return br.readLine();
    }

    public BufferedReader getBufferedReader() {
        return br;
    }
}
