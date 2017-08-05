package org.wing4j.rrd.net.protocol;

import lombok.Data;
import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.utils.HexUtils;

import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/8/4.
 * 断开连接协议
 */
@Data
public class RoundRobinDisConnectProtocolV1 extends BaseRoundRobinProtocol {
    int version = 1;
    ProtocolType protocolType = ProtocolType.DIS_CONNECT;
    MessageType messageType = MessageType.REQUEST;

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
    }
}
