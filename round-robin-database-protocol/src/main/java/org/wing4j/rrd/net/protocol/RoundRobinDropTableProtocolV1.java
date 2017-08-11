package org.wing4j.rrd.net.protocol;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.utils.HexUtils;

import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/8/4.
 * 删除表协议
 */
@Data
@ToString
public class RoundRobinDropTableProtocolV1 extends BaseRoundRobinProtocol {
    int version = 1;
    ProtocolType protocolType = ProtocolType.DROP_TABLE;
    MessageType messageType = MessageType.REQUEST;
    String instance = "default";
    String[] tableNames = new String[0];

    @Override
    public ByteBuffer convert() {
        ByteBuffer buffer = ByteBuffer.allocate(100);
        //网络传输协议
        //报文长度
        int lengthPos = buffer.position();
        buffer.putInt(0);
        //命令
        buffer = put(buffer, protocolType.getCode());
        if (DebugConfig.DEBUG) {
            System.out.println("protocol Type:" + protocolType);
        }
        //版本号
        buffer = put(buffer, version);
        if (DebugConfig.DEBUG) {
            System.out.println("version:" + version);
        }
        //请求类型
        buffer = put(buffer, messageType.getCode());
        if (DebugConfig.DEBUG) {
            System.out.println("message Type:" + messageType);
        }
        //应答编码
        buffer = put(buffer, code);
        if (DebugConfig.DEBUG) {
            System.out.println("code:" + code);
        }
        //应答描述
        buffer = put(buffer, desc);
        if (DebugConfig.DEBUG) {
            System.out.println("desc:" + desc);
        }
        //会话ID
        buffer = put(buffer, sessionId);
        //实例名长度
        //实例名
        buffer = put(buffer, instance);
        //表数目
        buffer = put(buffer, tableNames.length);
        for (int i = 0; i < tableNames.length; i++) {
            //表名长度
            //表名
            buffer = put(buffer, tableNames[i]);
        }
        //结束
        //回填,将报文总长度回填到第一个字节
        buffer.putInt(lengthPos, buffer.position() - 4);
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
        //实例名长度
        //实例名
        this.instance = get(buffer);
        //表数目
        int tableNum = buffer.getInt();
        this.tableNames = new String[tableNum];
        for (int i = 0; i < tableNum; i++) {
            //表名长度
            //表名
            this.tableNames[i] = get(buffer);
        }
    }
}
