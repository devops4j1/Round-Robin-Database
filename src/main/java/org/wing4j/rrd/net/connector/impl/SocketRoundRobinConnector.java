package org.wing4j.rrd.net.connector.impl;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.net.connector.RoundRobinConnector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

/**
 * Created by wing4j on 2017/7/31.
 */
@Data
@ToString
public class SocketRoundRobinConnector implements RoundRobinConnector {
    String address;
    int port;
    SocketChannel channel;

    public SocketRoundRobinConnector(String address, int port) throws IOException {
        this.address = address;
        this.port = port;
        SocketAddress remote = InetSocketAddress.createUnresolved(address, port);
        this.channel = SocketChannel.open(remote);
    }

    @Override
    public long[][] read(int time, int size, String... names) {
        return new long[0][];
    }

    @Override
    public RoundRobinConnector write(int time, long[][] data, String... names) {
        return this;
    }

    @Override
    public RoundRobinConnector start() {
        return this;
    }

    @Override
    public RoundRobinConnector close() {
        return this;
    }
}
