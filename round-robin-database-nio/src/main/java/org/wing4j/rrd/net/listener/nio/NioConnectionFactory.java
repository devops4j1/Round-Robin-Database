package org.wing4j.rrd.net.listener.nio;

import org.wing4j.rrd.server.RoundRobinServer;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;

/**
 * Created by wing4j on 2017/8/10.
 */
public class NioConnectionFactory {
    NioRoundRobinConnection getConnection(RoundRobinServer server, SocketChannel channel) throws IOException{
        return new NioRoundRobinConnection(server, channel);
    }

    public NioRoundRobinConnection make(RoundRobinServer server, SocketChannel channel) throws IOException {
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        NioRoundRobinConnection c = getConnection(server, channel);
        return c;
    }
}
