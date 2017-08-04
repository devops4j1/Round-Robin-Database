package org.wing4j.rrd.net.protocol;

import lombok.Data;
import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.utils.HexUtils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/8/2.
 * 合并视图协议
 */
@Data
public class RoundRobinMergeProtocolV1 implements RoundRobinProtocol {
    int version = 1;
    ProtocolType protocolType = ProtocolType.MERGE;
    static final boolean DEBUG = false;
    String[] columns = null;
    long[][] data = null;
    MergeType mergeType;
    String tableName;
    int current = 0;

    String fill(String in, boolean rightFillStyle, char fillChar, int len) {
        String str = in;
        while (str.length() < len) {
            str = rightFillStyle ? str + fillChar : fillChar + str;
        }
        return str;
    }

    @Override
    public ByteBuffer convert() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
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
        //网络传输协议
        fileSize += 4;//报文长度
        fileSize += 4;//命令
        fileSize += 4;//文件版本号
        fileSize += 4;//表名长度
        fileSize += tableNameLen;//表名
        fileSize += 4;//合并模式
        fileSize += 4;//文件时间指针
        fileSize += 4;//字段数量
        fileSize += 4;//字段长度
        fileSize += headerMaxLen * columns0.length * 4;//字段定义
        fileSize += columns0.length * data.length * 8;//数据区
        if (buffer.remaining() < fileSize) {
            //扩容
            ByteBuffer buffer1 = ByteBuffer.allocate(buffer.limit() + fileSize);
            buffer.flip();
            buffer1.put(buffer);
            buffer = buffer1;
        }
        int lengthPos = buffer.position();
        //int 整个报文长度
        buffer.putInt(0);
        //命令
        buffer.putInt(protocolType.getCode());
        if (DebugConfig.DEBUG) {
            System.out.println("protocol Type:" + protocolType);
        }
        //版本号
        buffer.putInt(version);
        if (DEBUG) {
            System.out.println("version:" + version);
        }
        //int 表名长度
        buffer.putInt(tableNameLen);
        //String 表名
        buffer.put(tableNameBytes);
        //int 合并模式
        buffer.putInt(mergeType.ordinal());
        //int 合并到的截至时间点
        buffer.putInt(current);
        if (DEBUG) {
            System.out.println("current:" + current);
        }
        buffer.putInt(columns0.length);
        if (DEBUG) {
            System.out.println("head size:" + columns0.length);
        }
        buffer.putInt(headerMaxLen);
        if (DEBUG) {
            System.out.println("head length:" + headerMaxLen);
        }
        for (int i = 0; i < columns0.length; i++) {
            columns0[i] = fill(columns0[i], true, ' ', headerMaxLen);
            char[] chars = columns0[i].toCharArray();
            for (char c : chars) {
                buffer.putChar(c);
            }
            if (DEBUG) {
                System.out.println("columns:" + columns0[i]);
            }
        }
        buffer.putInt(columns0.length);
        if (DEBUG) {
            System.out.println("head size:" + columns0.length);
        }
        buffer.putInt(data.length);
        if (DEBUG) {
            System.out.println("data size:" + data.length);
        }
        for (int i = 0; i < data.length; i++) {
            for (int j = 0; j < columns0.length; j++) {
                buffer.putLong(data[i][j]);
            }
        }
        if (DEBUG) {
            buffer.flip();
            byte[] data11 = new byte[buffer.limit()];
            buffer.get(data11);
            System.out.println(HexUtils.toDisplayString(data11));
        }
        //将报文总长度回填到第一个字节
        buffer.putInt(lengthPos, buffer.position() - 4);
        if (DEBUG) {
            System.out.println(HexUtils.toDisplayString(buffer.array()));
        }
        return buffer;
    }

    @Override
    public void convert(ByteBuffer buffer) {
        if (DEBUG) {
            System.out.println(HexUtils.toDisplayString(buffer.array()));
        }
        //网络传输协议
        //报文长度
        //命令
        //版本号
        //表名长度
        int tableNameLen = buffer.getInt();
        if (DEBUG) {
            System.out.println("tableNameLength:" + tableNameLen);
        }
        //表名
        byte[] tableNameBytes = new byte[tableNameLen];
        buffer.get(tableNameBytes);
        String tableName = new String(tableNameBytes);
        if (DEBUG) {
            System.out.println("tableName:" + tableName);
        }
        //合并模式
        int type = buffer.getInt();
        this.mergeType = MergeType.values()[type];

        //文件时间指针
        int current0 = buffer.getInt();
        if (DEBUG) {
            System.out.println("current:" + current0);
        }

        //字段数量
        int headerLen = buffer.getInt();
        if (DEBUG) {
            System.out.println("head size:" + headerLen);
        }
        //字段长度
        int headerMaxLen = buffer.getInt();
        if (DEBUG) {
            System.out.println("head length:" + headerMaxLen);
        }
        //字段定义
        String[] columns0 = new String[headerLen];
        for (int i = 0; i < columns0.length; i++) {
            char[] chars = new char[headerMaxLen];
            for (int j = 0; j < chars.length; j++) {
                chars[j] = buffer.getChar();
            }
            columns0[i] = new String(chars).trim();
            if (DEBUG) {
                System.out.println("columns:" + columns0[i]);
            }
        }
        //数据区
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
            for (int j = 0; j < columns0.length; j++) {
                data0[i][j] = buffer.getLong();
            }
        }
        this.tableName = tableName;
        this.current = current0;
        this.columns = columns0;
        this.data = data0;
    }
}
