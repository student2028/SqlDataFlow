package org.student.spark.common;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;


public class LogWriter extends Writer {
    final static Logger logger = LoggerFactory.getLogger(LogWriter.class);

    @Override
    public void write(char c[], int off, int len) {
        String str = new String(c, off, len);
        try {
            logger.info(str);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flush() throws IOException {
    }


    @Override
    public void close() throws IOException {
    }


}
