package org.wing4j.rrd.server.aio;

import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.server.RoundRobinServer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/8/1.
 * 环形数据库监听到客户端接入
 */
public class RoundRobinAcceptHandler implements CompletionHandler<AsynchronousSocketChannel, AsynchronousServerSocketChannel> {
    static Logger LOGGER = Logger.getLogger(AioRoundRobinServer.class.getName());
    RoundRobinDatabase database;

    public RoundRobinAcceptHandler(RoundRobinDatabase database) {
        this.database = database;
    }

    @Override
    public void completed(AsynchronousSocketChannel channel, AsynchronousServerSocketChannel attachment) {
        try {
            attachment.accept(attachment, this);
            LOGGER.info(Thread.currentThread().getName() + " " + channel.getRemoteAddress().toString() + " accept");
            ByteBuffer clientBuffer = ByteBuffer.allocate(4);
            channel.read(clientBuffer, clientBuffer, new RoundRobinReadHandler(channel, database));
        } catch (IOException e) {
            LOGGER.info(Thread.currentThread().getName() + " happens error! ");
        }
    }

    @Override
    public void failed(Throwable exc, AsynchronousServerSocketChannel attachment) {

    }
}
