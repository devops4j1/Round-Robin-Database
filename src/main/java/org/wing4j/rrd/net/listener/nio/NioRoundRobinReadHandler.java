package org.wing4j.rrd.net.listener.nio;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * Created by wing4j on 2017/8/10.
 */
public class NioRoundRobinReadHandler implements Runnable {
    final SocketChannel socket;
    final SelectionKey sk;
    static final int READING = 0;
    static final int SENDING = 1;
    int state = READING;

    public NioRoundRobinReadHandler(SocketChannel socket, SelectionKey sk) {
        this.socket = socket;
        this.sk = sk;
    }

    @Override
    public void run() {

    }
}
