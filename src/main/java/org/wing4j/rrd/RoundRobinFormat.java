package org.wing4j.rrd;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Created by wing4j on 2017/7/29.
 * 环状结构文件接口
 */
public interface RoundRobinFormat {
    long[][] getData();

    void setData(long[][] data);
    String getInstance();
    String getTableName();
    String[] getColumns();

    void setColumns(String[] columns);

    int getCurrent();

    void setCurrent(int current);

    int getVersion();

    void setVersion(int version);

    void read(File file) throws IOException;
    void read(String fileName) throws IOException;

    void write(String fileName) throws IOException;
    void read(ByteBuffer buffer);

    ByteBuffer write();

    ByteBuffer write(ByteBuffer buffer);
}
