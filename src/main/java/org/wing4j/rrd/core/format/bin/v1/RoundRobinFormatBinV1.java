package org.wing4j.rrd.core.format.bin.v1;

import lombok.Data;
import org.wing4j.rrd.RoundRobinFormat;
import org.wing4j.rrd.RoundRobinView;
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
    String[] header = null;
    long[][] data = null;
    static final boolean DEBUG = false;

    public RoundRobinFormatBinV1() {
    }
    public RoundRobinFormatBinV1(RoundRobinView view){
        this(view.getHeader(), view.getData(), view.getTime());
    }
    public RoundRobinFormatBinV1(String[] header, long[][] data, int current) {
        this.header = header;
        this.data = data;
        this.current = current;
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
        if (DEBUG) {
            System.out.println("version:" + version0);
        }
        int current0 = buffer.getInt();
        if (DEBUG) {
            System.out.println("current:" + current0);
        }
        int headerLen = buffer.getInt();
        if (DEBUG) {
            System.out.println("head size:" + headerLen);
        }
        int headerMaxLen = buffer.getInt();
        if (DEBUG) {
            System.out.println("head length:" + headerMaxLen);
        }
        String[] header0 = new String[headerLen];
        for (int i = 0; i < header0.length; i++) {
            char[] chars = new char[headerMaxLen];
            for (int j = 0; j < chars.length; j++) {
                chars[j] = buffer.getChar();
            }
            header0[i] = new String(chars).trim();
            if (DEBUG) {
                System.out.println("header:" + header0[i]);
            }
        }
        int dataLen0 = buffer.getInt();
        if (DEBUG) {
            System.out.println("head size:" + dataLen0);
        }
        int dataSize0 = buffer.getInt();
        if (DEBUG) {
            System.out.println("data size:" + dataSize0);
        }
        long[][] data0 = new long[dataSize0][dataLen0];
        for (int i = 0; i < data0.length; i++) {
            for (int j = 0; j < header0.length; j++) {
                data0[i][j] = buffer.getLong();
            }
        }
        this.version = version0;
        this.current = current0;
        this.header = header0;
        this.data = data0;
    }

    @Override
    public ByteBuffer write() {
        return write((ByteBuffer)null);
    }

    @Override
    public ByteBuffer write(ByteBuffer buffer) {
        int headerMaxLen = 0;
        String[] header0 = new String[header.length];
        for (int i = 0; i < header.length; i++) {
            header0[i] = header[i];
            if (header0[i].length() > headerMaxLen) {
                headerMaxLen = header0[i].length();
            }
        }
        int fileSize = 4;//文件版本号
        fileSize += 4;//文件时间指针
        fileSize += 4;//头数量
        fileSize += 4;//头长度
        fileSize += headerMaxLen * header0.length * 4;//文件头
        fileSize += header0.length * data.length * 8;//数据区
        if(buffer == null){
            buffer = ByteBuffer.allocate(fileSize);
        }
        buffer.putInt(version);
        if (DEBUG) {
            System.out.println("version:" + version);
        }
        buffer.putInt(current);
        if (DEBUG) {
            System.out.println("current:" + current);
        }
        buffer.putInt(header0.length);
        if (DEBUG) {
            System.out.println("head size:" + header0.length);
        }
        buffer.putInt(headerMaxLen);
        if (DEBUG) {
            System.out.println("head length:" + headerMaxLen);
        }
        for (int i = 0; i < header0.length; i++) {
            header0[i] = fill(header0[i], true, ' ', headerMaxLen);
            char[] chars = header0[i].toCharArray();
            for (char c : chars) {
                buffer.putChar(c);
            }
            if (DEBUG) {
                System.out.println("header:" + header0[i]);
            }
        }
        buffer.putInt(header0.length);
        if (DEBUG) {
            System.out.println("head size:" + header0.length);
        }
        buffer.putInt(data.length);
        if (DEBUG) {
            System.out.println("data size:" + header0.length);
        }
        for (int i = 0; i < data.length; i++) {
            for (int j = 0; j < header.length; j++) {
                buffer.putLong(data[i][j]);
            }
        }
        if (DEBUG) {
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
