package com.jasmine.processors.file_filter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

/**
 * File Writer class
 * */
public class FileWriter {
    private BufferedWriter bw;

    public FileWriter(OutputStream out) {
        this.bw = new BufferedWriter(new OutputStreamWriter(out));
    }

    public void closeFile() throws IOException {
        this.bw.close();
    }

    public void writeLine(String line) throws IOException {
        this.bw.write(line + "\n");
    }

    public BufferedWriter getBufferedWriter() {
        return bw;
    }
}
