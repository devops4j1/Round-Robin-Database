package org.wing4j.rrd.core.format.csv.v1;

import lombok.Data;
import org.wing4j.rrd.RoundRobinFormat;
import org.wing4j.rrd.RoundRobinRuntimeException;
import org.wing4j.rrd.RoundRobinView;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
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

    public RoundRobinFormatCsvV1(RoundRobinView view){
        this(view.getHeader(), view.getData(), view.getTime());
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

    @Override
    public void read(ByteBuffer buffer) {

    }

    @Override
    public ByteBuffer write() {
        return write((ByteBuffer)null);
    }

    @Override
    public ByteBuffer write(ByteBuffer buffer) {
        StringBuilder stringBuilder = new StringBuilder();
        for (String name : header) {
            stringBuilder.append(name).append(",");
        }
        stringBuilder.append("\n");
        for (int i = 0; i < data.length; i++) {
            for (int j = 0; j < header.length; j++) {
                stringBuilder.append(String.valueOf(data[i][j])).append(",");
            }
            stringBuilder.append("\n");
        }
        if(buffer == null){
            buffer = ByteBuffer.allocate(stringBuilder.length());
        }
        try {
            buffer.put(stringBuilder.toString().getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return buffer;
    }

    void write(WritableByteChannel channel) throws IOException {
        ByteBuffer buffer = write();
        buffer.flip();
        channel.write(buffer);
    }
}
