package org.wing4j.rrd.net.protocol;

import lombok.Data;
import org.wing4j.rrd.FormatType;
import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.utils.HexUtils;

import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/8/4.
 * 持久化表数据协议
 */
@Data
public class RoundRobinPersistentTableProtocolV1 extends BaseRoundRobinProtocol {
    int version = 1;
    ProtocolType protocolType = ProtocolType.PERSISTENT_TABLE;
    MessageType messageType = MessageType.REQUEST;
    String instance = "default";
    FormatType formatType;
    int formatVersion;
    String[] tableNames = new String[0];
    /**
     * 持久化时间，立即执行取值为0
     */
    int persistentTime = 0;
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
        //表数目
        buffer = put(buffer, tableNames.length);
        for (int i = 0; i < tableNames.length; i++) {
            //表名长度
            //表名
            buffer = put(buffer, tableNames[i]);
        }
        //持久化文件格式
        buffer = put(buffer, formatType.getCode());
        //文件版本
        buffer = put(buffer, formatVersion);
        //持久化时间
        buffer = put(buffer, persistentTime);
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
        //持久化文件格式
        this.formatType = FormatType.valueOfCode(buffer.getInt());
        //文件版本
        this.formatVersion = buffer.getInt();
        //持久化时间
        this.persistentTime = buffer.getInt();
    }
}
