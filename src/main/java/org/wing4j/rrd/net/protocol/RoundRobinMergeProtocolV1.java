package org.wing4j.rrd.net.protocol;

import lombok.Data;
import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.utils.HexUtils;
import org.wing4j.rrd.utils.MessageFormatter;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/8/2.
 * 合并视图协议
 */
@Data
public class RoundRobinMergeProtocolV1 extends BaseRoundRobinProtocol {
    static Logger LOGGER = Logger.getLogger(RoundRobinMergeProtocolV1.class.getName());
    ProtocolType protocolType = ProtocolType.MERGE;
    int version = 1;
    MessageType messageType = MessageType.REQUEST;
    String instance = "default";
    String tableName;
    String[] columns = null;
    long[][] data = new long[0][0];
    MergeType mergeType;
    int pos = 0;
    int size = 0;

    @Override
    public ByteBuffer convert() {
        //网络传输协议
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        //报文长度
        int lengthPos = buffer.position();
        buffer.putInt(0);
        //命令
        buffer.putInt(protocolType.getCode());
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("protocol Type:{}", protocolType));
        }
        //版本号
        buffer.putInt(version);
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("version:{}", version));
        }
        //报文类型
        buffer.putInt(messageType.getCode());
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("message Type:{}", messageType));
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
        buffer = put(buffer, this.tableName);
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("tableName:{}", this.tableName));
        }
        ///字段数
        buffer = put(buffer, this.columns.length);
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("column num:{}", this.columns.length));
        }
        //字段名
        for (int i = 0; i < this.columns.length; i++) {
            String column = this.columns[i];
            buffer = put(buffer, column);
        }
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("columns:{}", Arrays.toString(columns)));
        }
        //偏移地址
        buffer = put(buffer, this.pos);
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("pos:{}", pos));
        }
        //合并类型
        buffer = put(buffer, this.mergeType.getCode());
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("merge Type:{}", mergeType));
        }
        //记录条数
        buffer = put(buffer, this.size);
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("size:{}", size));
        }
        //数据
        for (int i = 0; i < this.size; i++) {
            for (int j = 0; j < this.columns.length; j++) {
                buffer = put(buffer, this.data[i][j]);
            }
        }
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("data:{}", Arrays.toString(data)));
        }
        //结束
        //回填,将报文总长度回填到第一个字节
        buffer.putInt(lengthPos, buffer.position() - 4);
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("报文总长度:{}", buffer.position() - 4));
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
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("tableName:{}", this.tableName));
        }
        //字段数目
        int columnNum = buffer.getInt();
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("column num:{}", columnNum));
        }
        this.columns = new String[columnNum];
        //字段
        for (int i = 0; i < columnNum; i++) {
            columns[i] = get(buffer);
        }
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("columns:{}", Arrays.toString(columns)));
        }
        //偏移地址
        this.pos = buffer.getInt();
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("pos:{}", pos));
        }
        //合并类型
        this.mergeType = MergeType.valueOfCode(buffer.getInt());
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("merge Type:{}", mergeType));
        }
        //记录条数
        this.size = buffer.getInt();
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("size:{}", size));
        }
        this.data = new long[this.size][columnNum];
        //数据
        for (int i = 0; i < this.size; i++) {
            for (int j = 0; j < columnNum; j++) {
                this.data[i][j] = buffer.getLong();
            }
        }
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("data:{}", Arrays.toString(data)));
        }
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("报文总长度:{}", buffer.position() - 4));
        }
    }
}
