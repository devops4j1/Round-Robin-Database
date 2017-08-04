package org.wing4j.rrd.net.protocol;

import lombok.Data;
import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.utils.HexUtils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/8/4.
 */
@Data
public class RoundRobinDataSizeProtocolV1 implements RoundRobinProtocol{
    String tableName;
    int version = 1;
    ProtocolType protocolType = ProtocolType.GET_DATA_SIZE;
    int dataSize;
    @Override
    public ByteBuffer convert() {
        ByteBuffer buffer = ByteBuffer.allocate(100);
        byte[] tableNameBytes = new byte[0];
        try {
            tableNameBytes = tableName.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        int tableNameLen = tableNameBytes.length;
        int fileSize = 0;
        //网络传输协议
        fileSize += 4;//报文长度
        fileSize += 4;//命令
        fileSize += 4;//文件版本号
        fileSize += 4;//表名长度
        fileSize += tableNameLen;//表名
        fileSize += 4;//数据长度
        if (buffer.remaining() < fileSize) {
            //扩容
            ByteBuffer buffer1 = ByteBuffer.allocate(buffer.limit() + fileSize);
            buffer.flip();
            buffer1.put(buffer);
            buffer = buffer1;
        }
        //int 整个报文长度
        int lengthPos = buffer.position();
        buffer.putInt(0);
        //命令
        buffer.putInt(protocolType.getCode());
        if (DebugConfig.DEBUG) {
            System.out.println("protocol Type:" + protocolType);
        }
        //版本号
        buffer.putInt(version);
        if (DebugConfig.DEBUG) {
            System.out.println("version:" + version);
        }
        //int 表名长度
        buffer.putInt(tableNameLen);
        //String 表名
        buffer.put(tableNameBytes);
        if (DebugConfig.DEBUG) {
            buffer.flip();
            byte[] data11 = new byte[buffer.limit()];
            buffer.get(data11);
            System.out.println(HexUtils.toDisplayString(data11));
        }
        //int 数据长度
        buffer.putInt(dataSize);
        //将报文总长度回填到第一个字节
        buffer.putInt(lengthPos, buffer.position() - 4);
        if (DebugConfig.DEBUG) {
            System.out.println(HexUtils.toDisplayString(buffer.array()));
        }
        return buffer;
    }

    @Override
    public void convert(ByteBuffer buffer) {
        if (DebugConfig.DEBUG) {
            System.out.println(HexUtils.toDisplayString(buffer.array()));
        }
        //网络传输协议
        //报文长度
        //命令
        //版本号
        //表名长度
        int tableNameLen = buffer.getInt();
        if (DebugConfig.DEBUG) {
            System.out.println("tableNameLength:" + tableNameLen);
        }
        //表名
        byte[] tableNameBytes = new byte[tableNameLen];
        buffer.get(tableNameBytes);
        String tableName = new String(tableNameBytes);
        if (DebugConfig.DEBUG) {
            System.out.println("tableName:" + tableName);
        }
        int dataSize0 = buffer.getInt();
        this.tableName = tableName;
        this.dataSize = dataSize0;
    }
}
