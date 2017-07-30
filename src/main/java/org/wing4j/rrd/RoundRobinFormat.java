package org.wing4j.rrd;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by wing4j on 2017/7/29.
 * 环状结构文件接口
 */
public interface RoundRobinFormat {
    long[][] getData();

    void setData(long[][] data);

    String[] getHeader();

    void setHeader(String[] header);

    int getCurrent();

    void setCurrent(int current);

    int getVersion();

    void setVersion(int version);

    void readFormFile(String fileName) throws IOException;

    void writeToFile(String fileName) throws IOException;
    void read(InputStream is) throws IOException;
    void write(OutputStream os) throws IOException;
}
