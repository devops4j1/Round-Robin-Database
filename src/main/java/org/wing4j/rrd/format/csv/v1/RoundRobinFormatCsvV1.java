package org.wing4j.rrd.format.csv.v1;

import lombok.Data;
import org.wing4j.rrd.RoundRobinFormat;
import org.wing4j.rrd.RoundRobinRuntimeException;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Created by wing4j on 2017/7/31.
 */
@Data
public class RoundRobinFormatCsvV1 implements RoundRobinFormat {
    int version = 1;
    int current = 0;
    String[] header = null;
    long[][] data = null;
    static final boolean DEBUG = false;

    public RoundRobinFormatCsvV1() {
    }

    public RoundRobinFormatCsvV1(String[] header, long[][] data, int current) {
        this.header = header;
        this.data = data;
        this.current = current;
    }

    public void read(String fileName) throws IOException {
        throw new RoundRobinRuntimeException("未实现");
    }

    public void write(String fileName) throws IOException {
        FileOutputStream fos = new FileOutputStream(fileName);
        FileChannel channel = null;
        try {
            channel = fos.getChannel();
            write(channel);
        } finally {
            if (channel != null) {
                channel.close();
            }
            if (fos != null) {
                fos.close();
            }
        }
    }

    public void read(ReadableByteChannel channel) throws IOException {
        throw new RoundRobinRuntimeException("未实现");
    }

    public void write(WritableByteChannel channel) throws IOException {
        StringBuilder buffer = new StringBuilder();
        for (String name : header) {
            buffer.append(name).append(",");
        }
        buffer.append("\n");
        for (int i = 0; i < data.length; i++) {
            for (int j = 0; j < header.length; j++) {
                buffer.append(String.valueOf(data[i][j])).append(",");
            }
            buffer.append("\n");
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer.toString().getBytes("UTF-8"));
        channel.write(byteBuffer);
    }
}
