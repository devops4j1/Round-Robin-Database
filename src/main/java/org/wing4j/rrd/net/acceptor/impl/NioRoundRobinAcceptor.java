package org.wing4j.rrd.net.acceptor.impl;

import org.wing4j.rrd.net.acceptor.RoundRobinAcceptor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

/**
 * Created by woate on 2017/8/8.
 */
public class NioRoundRobinAcceptor implements RoundRobinAcceptor {

    private final int port;
    private volatile Selector selector;
    private final ServerSocketChannel serverChannel;

    public NioRoundRobinAcceptor(int port) throws IOException {
        this.port = port;
        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.configureBlocking(false);
        /** 设置TCP属性 */
        serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        serverChannel.setOption(StandardSocketOptions.SO_RCVBUF, 1024 * 16 * 2);
        // backlog=100
        serverChannel.bind(new InetSocketAddress("localhost", port), 100);
        this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    @Override
    public void start() {

    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public int getPort() {
        return 0;
    }
}
