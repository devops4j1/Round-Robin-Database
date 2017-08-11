package org.wing4j.rrd.net.listener.aio;

import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.server.RoundRobinServerConfig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/8/1.
 * 环形数据库监听到客户端接入
 */
public class AioRoundRobinAcceptHandler implements CompletionHandler<AsynchronousSocketChannel, AsynchronousServerSocketChannel> {
    static Logger LOGGER = Logger.getLogger(AioRoundRobinAcceptHandler.class.getName());
    RoundRobinServerConfig serverConfig;
    RoundRobinDatabase database;

    public AioRoundRobinAcceptHandler(RoundRobinServerConfig serverConfig, RoundRobinDatabase database) {
        this.serverConfig = serverConfig;
        this.database = database;
    }

    @Override
    public void completed(AsynchronousSocketChannel channel, AsynchronousServerSocketChannel attachment) {
        LOGGER.info(Thread.currentThread().getName() + " 接受来自" + getAddress(channel) + "的请求");
        try {
            attachment.accept(attachment, this);
        } catch (AcceptPendingException e) {
            LOGGER.info(Thread.currentThread().getName() + " happens error! ");
            closeQuietly(channel);
            return;
        } catch (NotYetBoundException e) {
            LOGGER.info(Thread.currentThread().getName() + " happens error! ");
            closeQuietly(channel);
            return;
        } catch (ShutdownChannelGroupException e) {
            LOGGER.info(Thread.currentThread().getName() + " happens error! ");
            closeQuietly(channel);
            return;
        }

        ByteBuffer clientBuffer = ByteBuffer.allocate(4);
        try {
            channel.read(clientBuffer, clientBuffer, new AioRoundRobinReadHandler(channel, this.serverConfig, this.database));
        } catch (IllegalArgumentException e) {
            LOGGER.info(Thread.currentThread().getName() + " happens error! ");
            closeQuietly(channel);
            return;
        } catch (NotYetBoundException e) {
            LOGGER.info(Thread.currentThread().getName() + " happens error! ");
            closeQuietly(channel);
            return;
        } catch (ReadPendingException e) {
            LOGGER.info(Thread.currentThread().getName() + " happens error! ");
            closeQuietly(channel);
            return;
        } catch (ShutdownChannelGroupException e) {
            LOGGER.info(Thread.currentThread().getName() + " happens error! ");
            closeQuietly(channel);
            return;
        }
    }

    /**
     * 优雅地关闭套接字
     *
     * @param channel
     */
    void closeQuietly(AsynchronousSocketChannel channel) {
        try {
            if (channel != null && channel.isOpen()) {
                channel.shutdownInput();
                channel.shutdownOutput();
                channel.close();
            }
        } catch (IOException e) {
            LOGGER.warning("关闭套接字发生错误");
            e.printStackTrace();
        }
    }

    @Override
    public void failed(Throwable exc, AsynchronousServerSocketChannel attachment) {
        exc.printStackTrace();
    }

    String getAddress(AsynchronousSocketChannel address) {
        try {
            return address.getRemoteAddress().toString();
        } catch (IOException e) {
            return "获取IP失败";
        }
    }
}
