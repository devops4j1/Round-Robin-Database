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
    final RoundRobinServer server;
    final ServerSocketChannel serverChannel;
    NioRoundRobinReactorPool nioReactorPool;
    NioConnectionFactory factory;

    public NioRoundRobinListener(RoundRobinServer server, NioConnectionFactory factory, NioRoundRobinReactorPool nioReactorPool) throws IOException {
        setName("Nio-listener");
        this.server = server;
        this.port = server.getServerConfig().getListenPort();
        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.configureBlocking(false);
        this.factory = factory;
        this.nioReactorPool = nioReactorPool;
        //设置TCP属性
        serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);//
        serverChannel.setOption(StandardSocketOptions.SO_RCVBUF, 1024 * 16 * 2);//
        // backlog=100
        serverChannel.bind(new InetSocketAddress(port), 100);
        //服务端接收客户端连接事件
        this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
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
                //获取ACCEPT事件集合
                Set<SelectionKey> keys = tSelector.selectedKeys();
                //ACCEPT事件集合为空且两次阻塞事件小于最小选择阈值
                if (keys.size() == 0 && (end - start) <server.getServerConfig().getMinSelectTimeInNanoSeconds()) {
                    invalidSelectCount++;
                } else {
                    try {
                        for (SelectionKey key : keys) {
                            //ACCEPT事件有效且为ACCEPT可进行操作状态
                            if (key.isValid() && key.isAcceptable()) {
                                accept();
                            } else {
                                //其他事件或者不可操作状态，进行取消
                                key.cancel();
                            }
                        }
                    } finally {
                        //清空事件集合
                        keys.clear();
                        //无效的SELECT操作计数器清零
                        invalidSelectCount = 0;
                    }
                }
                //如果无效的SELECT操作超过阈值，则重建选择器
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
        SocketChannel channel = null;
        try {
            channel = serverChannel.accept();
            channel.configureBlocking(false);
            NioRoundRobinConnection connection = factory.make(server, channel);
            NioRoundRobinReactor reactor = nioReactorPool.getNextReactor();
            //在反射器注册后绑定连接信息，放入反射器处理连接队列
            reactor.postRegister(connection);
        } catch (Exception e) {
            //发生异常，关闭套接字、关闭通道
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
