package org.wing4j.rrd.core.format.bin.v1;

import lombok.Data;
import org.wing4j.rrd.RoundRobinFormat;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.utils.HexUtils;

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
public class RoundRobinFormatBinV1 implements RoundRobinFormat {
    int version = 1;
    int current = 0;
    String tableName;
    String[] columns = null;
    long[][] data = null;

    public RoundRobinFormatBinV1() {
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
        int version0 = buffer.getInt();
        if (DebugConfig.DEBUG) {
            System.out.println("version:" + version0);
        }
        int tableNameLen = buffer.getInt();
        if (DebugConfig.DEBUG) {
            System.out.println("tableNameLength:" + tableNameLen);
        }
        byte[] tableNameBytes = new byte[tableNameLen];
        buffer.get(tableNameBytes);
        String tableName = new String(tableNameBytes);
        if (DebugConfig.DEBUG) {
            System.out.println("tableName:" + tableName);
        }
        int current0 = buffer.getInt();
        if (DebugConfig.DEBUG) {
            System.out.println("current:" + current0);
        }
        int headerLen = buffer.getInt();
        if (DebugConfig.DEBUG) {
            System.out.println("head size:" + headerLen);
        }
        int headerMaxLen = buffer.getInt();
        if (DebugConfig.DEBUG) {
            System.out.println("head length:" + headerMaxLen);
        }
        String[] columns0 = new String[headerLen];
        for (int i = 0; i < columns0.length; i++) {
            char[] chars = new char[headerMaxLen];
            for (int j = 0; j < chars.length; j++) {
                chars[j] = buffer.getChar();
            }
            columns0[i] = new String(chars).trim();
            if (DebugConfig.DEBUG) {
                System.out.println("columns:" + columns0[i]);
            }
        }
        int dataLen0 = buffer.getInt();
        if (DebugConfig.DEBUG) {
            System.out.println("head size:" + dataLen0);
        }
        int dataSize0 = buffer.getInt();
        if (DebugConfig.DEBUG) {
            System.out.println("data size:" + dataSize0);
        }
        long[][] data0 = new long[dataSize0][dataLen0];
        for (int i = 0; i < data0.length; i++) {
            for (int j = 0; j < columns0.length; j++) {
                data0[i][j] = buffer.getLong();
            }
        }
        this.tableName = tableName;
        this.version = version0;
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
        byte[] tableNameBytes = new byte[0];
        try {
            tableNameBytes = tableName.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        int tableNameLen = tableNameBytes.length;
        int headerMaxLen = 0;
        String[] columns0 = new String[columns.length];
        for (int i = 0; i < columns0.length; i++) {
            columns0[i] = columns[i];
            if (columns0[i].length() > headerMaxLen) {
                headerMaxLen = columns0[i].length();
            }
        }
        int fileSize = 0;
        fileSize += 4;//文件版本号
        fileSize += 4;//表名长度
        fileSize += tableNameLen;//表名
        fileSize += 4;//文件时间指针
        fileSize += 4;//头数量
        fileSize += 4;//头长度
        fileSize += headerMaxLen * columns0.length * 4;//文件头
        fileSize += columns0.length * data.length * 8;//数据区
        if(buffer == null){
            buffer = ByteBuffer.allocate(fileSize);
        }
        buffer.putInt(version);
        if (DebugConfig.DEBUG) {
            System.out.println("version:" + version);
        }
        buffer.putInt(tableNameLen);
        buffer.put(tableNameBytes);
        buffer.putInt(current);
        if (DebugConfig.DEBUG) {
            System.out.println("current:" + current);
        }
        buffer.putInt(columns0.length);
        if (DebugConfig.DEBUG) {
            System.out.println("head size:" + columns0.length);
        }
        buffer.putInt(headerMaxLen);
        if (DebugConfig.DEBUG) {
            System.out.println("head length:" + headerMaxLen);
        }
        for (int i = 0; i < columns0.length; i++) {
            columns0[i] = fill(columns0[i], true, ' ', headerMaxLen);
            char[] chars = columns0[i].toCharArray();
            for (char c : chars) {
                buffer.putChar(c);
            }
            if (DebugConfig.DEBUG) {
                System.out.println("columns:" + columns0[i]);
            }
        }
        buffer.putInt(columns0.length);
        if (DebugConfig.DEBUG) {
            System.out.println("head size:" + columns0.length);
        }
        buffer.putInt(data.length);
        if (DebugConfig.DEBUG) {
            System.out.println("data size:" + data.length);
        }
        for (int i = 0; i < data.length; i++) {
            for (int j = 0; j < columns0.length; j++) {
                buffer.putLong(data[i][j]);
            }
        }
        if (DebugConfig.DEBUG) {
            buffer.flip();
            byte[] data11 = new byte[buffer.limit()];
            buffer.get(data11);
            System.out.println(HexUtils.toDisplayString(data11));
        }
        return buffer;
    }

    void read(ReadableByteChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(5 * 1024 * 1024);
        try {
            channel.read(buffer);
//            if (DEBUG) {
//                buffer.flip();
//                byte[] data11 = new byte[buffer.limit()];
//                buffer.get(data11);
//                System.out.println(HexUtils.toDisplayString(data11));
//            }
        } finally {
            if (channel != null) {
                channel.close();
            }
        }
        buffer.flip();
        read(buffer);
    }

    void write(WritableByteChannel channel) throws IOException {
        ByteBuffer buffer = write();
        buffer.flip();
        channel.write(buffer);
    }

    String fill(String in, boolean rightFillStyle, char fillChar, int len) {
        String str = in;
        while (str.length() < len) {
            str = rightFillStyle ? str + fillChar : fillChar + str;
        }
        return str;
    }
}
