package org.wing4j.rrd.net.listener.aio;

import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.server.RoundRobinServerConfig;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/8/1.
 */
public class AioRoundRobinWriteHandler implements CompletionHandler<Integer, ByteBuffer> {
    AsynchronousSocketChannel channel;
    RoundRobinServerConfig serverConfig;
    RoundRobinDatabase database;
    static Logger LOGGER = Logger.getLogger(AioRoundRobinWriteHandler.class.getName());

    public AioRoundRobinWriteHandler(AsynchronousSocketChannel channel, RoundRobinServerConfig serverConfig, RoundRobinDatabase database) {
        this.channel = channel;
        this.serverConfig = serverConfig;
        this.database = database;
    }

    @Override
    public void completed(Integer result, ByteBuffer attachment) {
        if (attachment.hasRemaining()) {//如果缓冲区未发送完成，继续进行发送
            channel.write(attachment, attachment, this);
        } else {//发送完则进行读取
            ByteBuffer readBuffer = ByteBuffer.allocate(1024);
            channel.read(readBuffer, readBuffer, new AioRoundRobinReadHandler(channel, this.serverConfig, this.database));
        }
    }

    @Override
    public void failed(Throwable exc, ByteBuffer attachment) {
        exc.printStackTrace();
    }
}
