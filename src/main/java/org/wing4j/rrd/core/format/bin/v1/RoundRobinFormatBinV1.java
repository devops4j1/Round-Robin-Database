package org.wing4j.rrd.core.format.bin.v1;

import lombok.Data;
import org.wing4j.rrd.FormatType;
import org.wing4j.rrd.RoundRobinFormat;
import org.wing4j.rrd.RoundRobinRuntimeException;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.core.format.BaseRoundRobinFormat;
import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.utils.HexUtils;
import org.wing4j.rrd.utils.MessageFormatter;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Created by wing4j on 2017/7/29.
 * 循环结构文件格式
 */
@Data
public class RoundRobinFormatBinV1 extends BaseRoundRobinFormat {
    int version = 1;
    int current = 0;
    String tableName;
    String[] columns = null;
    long[][] data = null;
    public RoundRobinFormatBinV1() {

    }
    public RoundRobinFormatBinV1(ByteBuffer buffer) {
        read(buffer);
    }
    public RoundRobinFormatBinV1(String tableName,RoundRobinView view){
        this(tableName, view.getMetadata().getColumns(), view.getData(), view.getTime());
    }
    public RoundRobinFormatBinV1(String tableName, String[] columns, long[][] data, int current) {
        this.tableName = tableName;
        this.columns = columns;
        this.data = data;
        this.current = current;
    }

    @Override
    public void read(File file) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        FileChannel fileChannel = fis.getChannel();
        try {
            read(fileChannel);
        } finally {
            if (fileChannel != null) {
                fileChannel.close();
            }
            if (fis != null) {
                fis.close();
            }
        }
    }

    public void read(String fileName) throws IOException {
        FileInputStream fis = new FileInputStream(fileName);
        FileChannel fileChannel = fis.getChannel();
        try {
            read(fileChannel);
        } finally {
            if (fileChannel != null) {
                fileChannel.close();
            }
            if (fis != null) {
                fis.close();
            }
        }
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
        //总长度
        //文件版本号
        //文件格式
        //表名
        String tableName = get(buffer);
        //字段数
        int columnLen = buffer.getInt();
        String[] columns0 = new String[columnLen];
        //字段名
        for (int i = 0; i < columnLen; i++) {
            columns0[i] = get(buffer);
        }
        //数据行数
        int dataSize0 = buffer.getInt();
        long[][] data0 = new long[dataSize0][columnLen];
        for (int i = 0; i < dataSize0; i++) {
            for (int j = 0; j < columnLen; j++) {
                data0[i][j] = buffer.getLong();
            }
        }
        //文件时间指针
        int current0 = buffer.getInt();
        this.tableName = tableName;
        this.version = 1;
        this.current = current0;
        this.columns = columns0;
        this.data = data0;
    }

    @Override
    public ByteBuffer write() {
        return write((ByteBuffer)null);
    }

    @Override
    public ByteBuffer write(ByteBuffer buffer) {
        if(buffer == null){
            buffer = ByteBuffer.allocate(1024);
        }
        //文件格式
        buffer = put(buffer, FormatType.BIN.getCode());
        //总长度
        int lengthPos = buffer.position();
        buffer = put(buffer, 0);
        //文件版本号
        buffer = put(buffer, version);
        //表名
        buffer = put(buffer, tableName);
        //字段数
        buffer = put(buffer, columns.length);
        //字段名
        for (int i = 0; i < columns.length; i++) {
            buffer = put(buffer, columns[i]);
        }
        //数据行数
        buffer = put(buffer, data.length);
        for (int i = 0; i < data.length; i++) {
            for (int j = 0; j < columns.length; j++) {
                buffer = put(buffer, data[i][j]);
            }
        }
        //文件时间指针
        buffer = put(buffer, current);
        //结束
        int formatLen = buffer.position() - 4 - 4;
        //回填,将报文总长度回填到第一个字节
        buffer.putInt(lengthPos, formatLen);
        return buffer;
    }

    void read(ReadableByteChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(5 * 1024 * 1024);
        try {
            channel.read(buffer);
        } finally {
            if (channel != null) {
                channel.close();
            }
        }
        buffer.flip();
        int format = buffer.getInt();
        if(FormatType.valueOfCode(format) != FormatType.BIN){
            throw new RoundRobinRuntimeException("不支持的数据格式");
        }
        int fileLen = buffer.getInt();
        if(buffer.remaining() < fileLen){
            throw new RoundRobinRuntimeException("文件格式不正确");
        }
        int version = buffer.getInt();
        if(version != 1){
            throw new RoundRobinRuntimeException("不支持的版本");
        }
        read(buffer);
    }

    void write(WritableByteChannel channel) throws IOException {
        ByteBuffer buffer = write();
        buffer.flip();
        channel.write(buffer);
    }
}
