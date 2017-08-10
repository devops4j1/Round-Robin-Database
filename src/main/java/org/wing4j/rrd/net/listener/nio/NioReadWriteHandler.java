package org.wing4j.rrd.net.listener.nio;

import org.wing4j.rrd.net.listener.RoundRobinReadWriteHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/8/10.
 */
public class NioReadWriteHandler implements RoundRobinReadWriteHandler {
    static Logger LOGGER = Logger.getLogger(NioReadWriteHandler.class.getName());
    private SelectionKey processKey;
    private static final int OP_NOT_READ = ~SelectionKey.OP_READ;
    private static final int OP_NOT_WRITE = ~SelectionKey.OP_WRITE;
    final NioRoundRobinConnection connection;
    final SocketChannel channel;
    final AtomicBoolean writing = new AtomicBoolean(false);


    public NioReadWriteHandler(NioRoundRobinConnection connection) {
        this.connection = connection;
        this.channel = (SocketChannel) connection.getChannel();
    }

    @Override
    public void register(Selector selector) throws IOException {
        try {
            processKey = channel.register(selector, SelectionKey.OP_READ, connection);
        } finally {
            if (connection.isClosed()) {
                clearSelectionKey();
            }
        }
    }

    @Override
    public void asyncRead() throws IOException {
        ByteBuffer readBuffer = connection.getReadBuffer();
        //读取缓冲区，返回已读取字节数
        int got = channel.read(readBuffer);
        //对已读取字节数进行处理
        connection.onReadData(got);
    }

    @Override
    public void doNextWriteCheck() {
        System.out.println(Thread.currentThread().getName() + " doNextWriteCheck");
    }

    void clearSelectionKey() {
        try {
            SelectionKey key = this.processKey;
            if (key != null && key.isValid()) {
                key.attach(null);
                key.cancel();
            }
        } catch (Exception e) {
            LOGGER.warning("clear selector keys err:" + e);
        }
    }
}
