package org.wing4j.rrd.net.listener.nio;

import lombok.Getter;
import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.net.ClosableConnection;
import org.wing4j.rrd.net.cmd.RoundRobinCommandDispatcher;
import org.wing4j.rrd.net.listener.RoundRobinReadWriteHandler;
import org.wing4j.rrd.net.protocol.MessageType;
import org.wing4j.rrd.net.protocol.ProtocolType;
import org.wing4j.rrd.server.RoundRobinServer;
import org.wing4j.rrd.server.authority.impl.SimpleAuthorityService;
import org.wing4j.rrd.utils.HexUtils;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/8/10.
 */
public class NioRoundRobinConnection implements ClosableConnection {
    static Logger LOGGER = Logger.getLogger(NioRoundRobinConnection.class.getName());
    final AtomicBoolean closed;
    RoundRobinCommandDispatcher dispatcher;
    RoundRobinServer server;
    @Getter
    protected final SocketChannel channel;
    @Getter
    protected volatile ByteBuffer readBuffer = null;
    @Getter
    protected volatile ByteBuffer writeBuffer = null;
    @Getter
    protected final RoundRobinReadWriteHandler readWriteHandler;
    @Getter
    protected final ConcurrentLinkedQueue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<ByteBuffer>();
    @Getter
    protected volatile int readBufferOffset;
    @Getter
    protected long netInBytes;
    @Getter
    protected long netOutBytes;
    @Getter
    protected int writeAttempts;
    @Getter
    protected long lastReadTime;
    @Getter
    protected long lastWriteTime;

    public NioRoundRobinConnection(RoundRobinServer server, SocketChannel channel) {
        this.channel = channel;
        this.closed = new AtomicBoolean(false);
        boolean aio = (channel instanceof AsynchronousChannel);
        if (aio) {
            this.readWriteHandler = null;
        } else {
            this.readWriteHandler = new NioReadWriteHandler(this);
        }
        this.server = server;
        this.dispatcher = new RoundRobinCommandDispatcher(server.getServerConfig(), new SimpleAuthorityService(server.getServerConfig()));
    }

    public void asyncRead() throws IOException {
        if (!channel.isOpen()) {
            this.closed.set(true);
            close("通道已经关闭");
            return;
        }
        readWriteHandler.asyncRead();
    }

    protected int getPacketLength(ByteBuffer buffer, int offset) {
        return buffer.getInt(offset);
    }

    ByteBuffer compactReadBuffer(ByteBuffer buffer, int offset) {
        if (buffer == null) {
            return null;
        }
        buffer.limit(buffer.position());
        buffer.position(offset);
        buffer = buffer.compact();
        readBufferOffset = 0;
        return buffer;
    }

    public void onReadData(int got) throws IOException {
        if (isClosed()) {
            return;
        }
        if (got < 0) {
            this.close("stream closed");
            return;
        } else if (got == 0 && !this.channel.isOpen()) {
            this.close("socket closed");
            return;
        }
        netInBytes += got;
        // 循环处理字节信息
        int offset = readBufferOffset, length = 0, position = readBuffer.position();
        for (; ; ) {
            length = getPacketLength(readBuffer, offset);
            if (length <= 0) {
                if (offset != 0) {
                    this.readBuffer = compactReadBuffer(readBuffer, offset);
                } else if (readBuffer != null && !readBuffer.hasRemaining()) {
                    throw new RuntimeException("invalid readbuffer capacity ,too little buffer size "
                            + readBuffer.capacity());
                }
                break;
            }

            if (position >= offset + length && readBuffer != null) {

                // handle this package
                readBuffer.position(offset + 4);
                byte[] data = new byte[length];
                readBuffer.get(data, 0, length);
                //处理数据请求
                handle(data);
                // maybe handle stmt_close
                if (isClosed()) {
                    return;
                }

                // offset to next position
                offset += length;

                // reached end
                if (position == offset) {
                    // if cur buffer is temper none direct byte buffer and not
                    // received large message in recent 30 seconds
                    // then change to direct buffer for performance
//                    if (readBuffer != null && !readBuffer.isDirect()
//                            && lastLargeMessageTime < lastReadTime - 30 * 1000L) {  // used temp heap
//                        if (LOGGER.isDebugEnabled()) {
//                            LOGGER.debug("change to direct con read buffer ,cur temp buf size :" + readBuffer.capacity());
//                        }
//                        recycle(readBuffer);
//                        readBuffer = ByteBuffer.allocate(1024);
//                    } else {
                    if (readBuffer != null) {
                        readBuffer.clear();
                    }
//                    }
                    // no more data ,break
                    readBufferOffset = 0;
                    break;
                } else {
                    // try next package parse
                    readBufferOffset = offset;
                    if (readBuffer != null) {
                        readBuffer.position(position);
                    }
                    continue;
                }


            } else {
                // not read whole message package ,so check if buffer enough and
                // compact readbuffer
                if (!readBuffer.hasRemaining()) {
                    readBuffer = ensureFreeSpaceOfReadBuffer(readBuffer, offset, length);
                }
                break;
            }
        }
    }

    ByteBuffer ensureFreeSpaceOfReadBuffer(ByteBuffer buffer,
                                           int offset, final int pkgLength) {
        // need a large buffer to hold the package
        if (pkgLength > 3 * 1024 * 1024) {
            throw new IllegalArgumentException("Packet size over the limit.");
        } else if (buffer.capacity() < pkgLength) {
            ByteBuffer newBuffer = ByteBuffer.allocate(pkgLength);
            buffer.position(offset);
            newBuffer.put(buffer);
            readBuffer = newBuffer;
            recycle(buffer);
            readBufferOffset = 0;
            return newBuffer;
        } else {
            if (offset != 0) {
                // compact bytebuffer only
                return compactReadBuffer(buffer, offset);
            } else {
                throw new RuntimeException(" not enough space");
            }
        }
    }

    public void handle(byte[] data) {
        ByteBuffer attachment = ByteBuffer.wrap(data);
        ByteBuffer resultBuffer = null;
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
        if (messageType != MessageType.REQUEST) {
            LOGGER.info("报文格式不为请求报文格式");
            resultBuffer = ByteBuffer.wrap(" message is not request format!".getBytes());
            //注册异步写入返回信息
            write(resultBuffer);
            return;
        }
        try {
            resultBuffer = this.dispatcher.dispatch(protocolType, version, attachment, this.server.getDatabase());
        } catch (Exception e) {
            resultBuffer = ByteBuffer.wrap("database happens unknown error!".getBytes());
        } finally {
            if (DebugConfig.DEBUG) {
                byte[] data0 = new byte[resultBuffer.limit()];
                resultBuffer.get(data0);
                System.out.println(HexUtils.toDisplayString(data0));
                resultBuffer.flip();
            }
            //注册异步写入返回信息
            write(resultBuffer);
        }
    }

    @Override
    public String getCharset() {
        return null;
    }

    public void close(String cause) {
        closeSocket();
        cleanUp();
        closed.set(true);
        if (cause == null || cause.isEmpty()) {
            return;
        }
    }

    void closeSocket() {
        if (channel != null) {
            if (channel instanceof SocketChannel) {
                Socket socket = ((SocketChannel) channel).socket();
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        LOGGER.warning("closeChannelError" + e);
                    }
                }
            }

            boolean socketClosed = true;
            try {
                channel.close();
            } catch (Exception e) {
                socketClosed = false;
                LOGGER.warning("AbstractConnectionCloseError" + e);
            }

            boolean closed = socketClosed && (!channel.isOpen());
            if (closed == false) {
                LOGGER.warning("close socket of connnection failed " + this);
            }
        }
    }

    /**
     * 回收资源
     */
    void cleanUp() {
        // 清理资源占用
        if (readBuffer != null) {
            this.recycle(readBuffer);
            this.readBuffer = null;
            this.readBufferOffset = 0;
        }

        if (writeBuffer != null) {
            recycle(writeBuffer);
            this.writeBuffer = null;
        }
        ByteBuffer buffer = null;
        while ((buffer = writeQueue.poll()) != null) {
            recycle(buffer);
        }
    }

    public final void recycle(ByteBuffer buffer) {

    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void idleCheck() {

    }

    @Override
    public long getStartupTime() {
        return 0;
    }

    @Override
    public String getHost() {
        return null;
    }

    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public int getLocalPort() {
        return 0;
    }

    @Override
    public long getNetOutBytes() {
        return 0;
    }

    public void doNextWriteCheck() {
        readWriteHandler.doNextWriteCheck();
    }

    public void register() throws IOException {
        if (!isClosed()) {
            asyncRead();
        }
    }

    public void write(ByteBuffer buffer) {
        writeQueue.offer(buffer);
        // if ansyn write finishe event got lock before me ,then writing
        // flag is set false but not start a write request
        // so we check again
        try {
            this.readWriteHandler.doNextWriteCheck();
        } catch (Exception e) {
            e.printStackTrace();
            this.close("write err:" + e);
        }
    }
}
