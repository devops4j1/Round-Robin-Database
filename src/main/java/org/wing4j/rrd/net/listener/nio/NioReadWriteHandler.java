package org.wing4j.rrd.net.listener.nio;

import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.net.listener.RoundRobinReadWriteHandler;
import org.wing4j.rrd.utils.HexUtils;

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

     boolean write0() throws IOException {
        int written = 0;
        ByteBuffer buffer = connection.getWriteBuffer();
        if (buffer != null) {
            while (buffer.hasRemaining()) {
                written = channel.write(buffer);
                if (written > 0) {
                    connection.netOutBytes += written;
                    connection.lastWriteTime = System.currentTimeMillis();
                } else {
                    break;
                }
            }
            if(DebugConfig.DEBUG){
                buffer.flip();
                byte[] data0 = new byte[buffer.limit()];
                System.out.println("write:" + HexUtils.toDisplayString(data0));
            }
            if (buffer.hasRemaining()) {
                connection.writeAttempts++;
                return false;
            } else {
                connection.writeBuffer = null;
                connection.recycle(buffer);
            }
        }
        while ((buffer = connection.getWriteQueue().poll()) != null) {
            if (buffer.limit() == 0) {
                connection.recycle(buffer);
                connection.close("quit send");
                return true;
            }

            buffer.flip();
            try {
                while (buffer.hasRemaining()) {
                    written = channel.write(buffer);// java.io.IOException:
                    // Connection reset by peer
                    if (written > 0) {
                        connection.netOutBytes += written;
                        connection.lastWriteTime = System.currentTimeMillis();
                    } else {
                        break;
                    }
                }
            } catch (IOException e) {
                connection.recycle(buffer);
                throw e;
            }
            if (buffer.hasRemaining()) {
                connection.writeBuffer = buffer;
                connection.writeAttempts++;
                return false;
            } else {
                connection.recycle(buffer);
            }
        }
        return true;
    }

     void disableWrite() {
        try {
            SelectionKey key = this.processKey;
            key.interestOps(key.interestOps() & OP_NOT_WRITE);
        } catch (Exception e) {
           LOGGER.warning("can't disable write " + e + " connection " + connection);
        }

    }

     void enableWrite(boolean wakeup) {
        boolean needWakeup = false;
        try {
            SelectionKey key = this.processKey;
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            needWakeup = true;
        } catch (Exception e) {
            LOGGER.warning("can't enable write " + e);

        }
        if (needWakeup && wakeup) {
            processKey.selector().wakeup();
        }
    }

     void disableRead() {

        SelectionKey key = this.processKey;
        key.interestOps(key.interestOps() & OP_NOT_READ);
    }

    public void enableRead() {

        boolean needWakeup = false;
        try {
            SelectionKey key = this.processKey;
            key.interestOps(key.interestOps() | SelectionKey.OP_READ);
            needWakeup = true;
        } catch (Exception e) {
            LOGGER.warning("enable read fail " + e);
        }
        if (needWakeup) {
            processKey.selector().wakeup();
        }
    }
    @Override
    public void doNextWriteCheck() {
        if (!writing.compareAndSet(false, true)) {
            return;
        }

        try {
            boolean noMoreData = write0();
            writing.set(false);
            if (noMoreData && connection.getWriteQueue().isEmpty()) {
                if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) != 0)) {
                    disableWrite();
                }

            } else {

                if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) == 0)) {
                    enableWrite(false);
                }
            }

        } catch (IOException e) {
            LOGGER.warning("caught err:" + e);
            connection.close("err:" + e);
        }
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
