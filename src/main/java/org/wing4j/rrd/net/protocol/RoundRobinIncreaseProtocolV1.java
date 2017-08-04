package org.wing4j.rrd.net.protocol;

import lombok.Data;
import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.utils.HexUtils;

import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/8/4.
 * 字段自增协议
 */
@Data
public class RoundRobinIncreaseProtocolV1 extends BaseRoundRobinProtocol {
    String tableName;
    int version = 1;
    ProtocolType protocolType = ProtocolType.INCREASE;
    String column;
    int value;
    long newValue;

    @Override
    public ByteBuffer convert() {
        ByteBuffer buffer = ByteBuffer.allocate(100);
        //网络传输协议
        //报文长度
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
        //表名长度
        //表名
        buffer = put(buffer, tableName);
        //自增字段名长度
        //自增字段名
        buffer = put(buffer, column);
        //自增量
        buffer.putInt(value);
        if (DebugConfig.DEBUG) {
            System.out.println("value:" + value);
        }
        //自增后新值
        buffer.putLong(newValue);
        if (DebugConfig.DEBUG) {
            System.out.println("newValue:" + newValue);
        }
        //结束
        //回填,将报文总长度回填到第一个字节
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
        //表名
        this.tableName = get(buffer);
        //自增字段名长度
        //自增字段名
        this.column = get(buffer);
        //自增量
        this.value = buffer.getInt();
        if (DebugConfig.DEBUG) {
            System.out.println("value:" + value);
        }
        //自增后新值
        this.newValue = buffer.getLong();
        if (DebugConfig.DEBUG) {
            System.out.println("newValue:" + newValue);
        }
    }
}
