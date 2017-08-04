package org.wing4j.rrd.net.connector.impl;

import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.net.connector.RoundRobinConnector;

import java.io.IOException;

/**
 * Created by wing4j on 2017/8/2.
 */
public class NioRoundRobinConnector implements RoundRobinConnector {
    String address;
    int port;

    public NioRoundRobinConnector(String address, int port) {
        this.address = address;
        this.port = port;
    }

    @Override
    public String getAddress() {
        return null;
    }

    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public RoundRobinView read(int time, int size, String tableName, String... names) {
        return null;
    }

    @Override
    public RoundRobinConnector write(String tableName, int time,RoundRobinView view, MergeType mergeType) throws IOException {
        return null;
    }
}
