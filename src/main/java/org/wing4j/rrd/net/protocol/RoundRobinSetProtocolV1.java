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
public class RoundRobinSetProtocolV1 extends BaseRoundRobinProtocol {
    int version = 1;
    ProtocolType protocolType = ProtocolType.SET;
    MessageType messageType = MessageType.REQUEST;
    String instance;
    String tableName;
    String column;
    int pos;
    long value;
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
        //报文类型
        buffer.putInt(messageType.getCode());
        if (DebugConfig.DEBUG) {
            System.out.println("message Type:" + messageType);
        }
        //应答编码
        buffer = put(buffer, code);
        //应答描述
        buffer = put(buffer, desc);
        //会话ID
        buffer = put(buffer, sessionId);
        //实例名长度
        //实例名
        buffer = put(buffer, instance);
        //表名长度
        //表名
        buffer = put(buffer, tableName);
        //自增字段名长度
        //自增字段名
        buffer = put(buffer, column);
        //偏移地址
        buffer = put(buffer, pos);
        //设置值
        buffer = put(buffer, value);
        if (DebugConfig.DEBUG) {
            System.out.println("value:" + value);
        }
        //设置后新值
        buffer = put(buffer, newValue);
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
        //报文类型
        //应答编码
        this.code = buffer.getInt();
        //应答描述
        this.desc = get(buffer);
        //会话ID
        this.sessionId = get(buffer);
        //实例长度
        //实例
        this.instance = get(buffer);
        //表名长度
        //表名
        this.tableName = get(buffer);
        //自增字段名长度
        //自增字段名
        this.column = get(buffer);
        //偏移地址
        this.pos = buffer.getInt();
        //设置值
        this.value = buffer.getLong();
        if (DebugConfig.DEBUG) {
            System.out.println("value:" + value);
        }
        //设置后新值
        this.newValue = buffer.getLong();
        if (DebugConfig.DEBUG) {
            System.out.println("newValue:" + newValue);
        }
    }
}
