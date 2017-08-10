package org.wing4j.rrd.net.listener.nio;

import lombok.Getter;
import org.wing4j.rrd.net.listener.RoundRobinListener;
import org.wing4j.rrd.net.utils.SelectorUtil;
import org.wing4j.rrd.server.RoundRobinServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/8/8.
 */
public class NioRoundRobinListener extends Thread implements RoundRobinListener {
    static Logger LOGGER = Logger.getLogger(NioRoundRobinListener.class.getName());
    @Getter
    private final int port;
    //已接受连接计数器
    int acceptCount;
    private volatile Selector selector;
    RoundRobinServer server;
    private final ServerSocketChannel serverChannel;

    public NioRoundRobinListener(RoundRobinServer server) throws IOException {
        setName("Nio-listener");
        this.server = server;
        this.port = server.getServerConfig().getListenPort();
        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.configureBlocking(false);
        //设置TCP属性
        serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);//
        serverChannel.setOption(StandardSocketOptions.SO_RCVBUF, 1024 * 16 * 2);//
        // backlog=100
        serverChannel.bind(new InetSocketAddress("localhost", port), 100);
        this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);//服务端接收客户端连接事件
    }

    @Override
    public void run() {
        //无线的选择器计数器
        int invalidSelectCount = 0;
        for (; ; ) {
            final Selector tSelector = this.selector;
            ++acceptCount;
            try {
                long start = System.nanoTime();
                tSelector.select(1000L);
                long end = System.nanoTime();
                Set<SelectionKey> keys = tSelector.selectedKeys();
                if (keys.size() == 0 && (end - start) <server.getServerConfig().getMinSelectTimeInNanoSeconds()) {
                    invalidSelectCount++;
                } else {
                    try {
                        for (SelectionKey key : keys) {
                            //接收请求
                            if (key.isValid() && key.isAcceptable()) {
                                accept();
                            } else {
                                key.cancel();
                            }
                        }
                    } finally {
                        keys.clear();
                        invalidSelectCount = 0;
                    }
                }
                if (invalidSelectCount > server.getServerConfig().getRebuildCountThreshold()) {
                    final Selector rebuildSelector = SelectorUtil.rebuildSelector(this.selector);
                    if (rebuildSelector != null) {
                        this.selector = rebuildSelector;
                    }
                    invalidSelectCount = 0;
                }
            } catch (Exception e) {
                LOGGER.warning(getName() + e);
            }
        }
    }

    void accept() {
        System.out.println("------------------------accept");
        SocketChannel channel = null;
        try {
            channel = serverChannel.accept();
            channel.configureBlocking(false);
            channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
            new NioRoundRobinReadHandler(channel, );
            //TODO 读取套接字通道
        } catch (Exception e) {
            LOGGER.warning(getName() + e);
            if (channel == null) {
                return;
            }
            Socket socket = channel.socket();
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e1) {
                    LOGGER.warning("closeChannelError" + e1);
                }
            }
            try {
                channel.close();
            } catch (IOException e1) {
                LOGGER.warning("closeChannelError" + e1);
            }
        }
    }



}
