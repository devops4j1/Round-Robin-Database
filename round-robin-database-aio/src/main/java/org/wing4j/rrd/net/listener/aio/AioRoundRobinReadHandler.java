package org.wing4j.rrd.net.listener.aio;

import org.wing4j.rrd.*;
import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.net.protocol.*;
import org.wing4j.rrd.server.RoundRobinServerConfig;
import org.wing4j.rrd.net.cmd.RoundRobinCommandDispatcher;
import org.wing4j.rrd.server.authority.impl.SimpleAuthorityService;
import org.wing4j.rrd.utils.HexUtils;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/8/1.
 */
public class AioRoundRobinReadHandler implements CompletionHandler<Integer, ByteBuffer> {
    static Logger LOGGER = Logger.getLogger(AioRoundRobinReadHandler.class.getName());
    AsynchronousSocketChannel channel;
    RoundRobinServerConfig serverConfig;
    RoundRobinDatabase database;
    RoundRobinCommandDispatcher dispatcher;

    public AioRoundRobinReadHandler(AsynchronousSocketChannel channel, RoundRobinServerConfig serverConfig, RoundRobinDatabase database) {
        this.channel = channel;
        this.database = database;
        this.dispatcher = new RoundRobinCommandDispatcher(serverConfig, new SimpleAuthorityService(serverConfig));
    }
    @Override
    public void completed(Integer result, ByteBuffer attachment) {
        if (result < 1) {
            return;
        }
        attachment.flip();
        int size = attachment.getInt();
        if (DebugConfig.DEBUG){
            System.out.println("接收到" + size + "字节报文");
        }
        attachment = ByteBuffer.allocate(size);
        Future future = channel.read(attachment);
        ByteBuffer resultBuffer = null;
        try {
            future.get();
        } catch (Exception e) {
            resultBuffer = ByteBuffer.wrap("database happens unknown error!".getBytes());

            //注册异步写入返回信息
            channel.write(resultBuffer, resultBuffer, new AioRoundRobinWriteHandler(channel, this.serverConfig, this.database));
            return;
        }
        attachment.flip();
        if (attachment.remaining() < size) {
            LOGGER.info("无效的报文格式");
            resultBuffer = ByteBuffer.wrap("illegal message format!".getBytes());
            //注册异步写入返回信息
            channel.write(resultBuffer, resultBuffer, new AioRoundRobinWriteHandler(channel, this.serverConfig, this.database));
            return;
        }
        //命令类型
        int type = attachment.getInt();
        ProtocolType protocolType = ProtocolType.valueOfCode(type);
        //通信协议版本号
        int version = attachment.getInt();
        if (DebugConfig.DEBUG) {
            LOGGER.info("命令:" + protocolType.getDesc() + "." + version);
        }
        //报文类型
        int messageType0 = attachment.getInt();
        MessageType messageType = MessageType.valueOfCode(messageType0);
        if (DebugConfig.DEBUG) {
            LOGGER.info("报文类型:" + messageType);
        }
        if(messageType != MessageType.REQUEST){
            LOGGER.info("报文格式不为请求报文格式");
            resultBuffer = ByteBuffer.wrap(" message is not request format!".getBytes());
            //注册异步写入返回信息
            channel.write(resultBuffer, resultBuffer, new AioRoundRobinWriteHandler(channel, this.serverConfig, this.database));
            return;
        }
        try {
            resultBuffer = this.dispatcher.dispatch(protocolType, version, attachment, this.database);
        }catch (Exception e){
            resultBuffer = ByteBuffer.wrap("database happens unknown error!".getBytes());
        }finally {
            if (DebugConfig.DEBUG) {
                byte[] data = new byte[resultBuffer.limit()];
                resultBuffer.get(data);
                System.out.println(HexUtils.toDisplayString(data));
                resultBuffer.flip();
            }
            //注册异步写入返回信息
            channel.write(resultBuffer, resultBuffer, new AioRoundRobinWriteHandler(channel, this.serverConfig, this.database));
        }


    }

    @Override
    public void failed(Throwable exc, ByteBuffer attachment) {
        exc.printStackTrace();
    }
}
