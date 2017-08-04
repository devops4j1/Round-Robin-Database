package org.wing4j.rrd.net.connector.impl;

import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.core.TableMetadata;
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
    public TableMetadata getTableMetadata(String tableName) throws IOException {
        return null;
    }

    @Override
    public int getDataSize(String tableName) throws IOException {
        return 0;
    }

    @Override
    public long increase(String tableName, String column, int i) throws IOException {
        return 0;
    }

    @Override
    public RoundRobinView read(int size, String tableName, String... columns) throws IOException {
        return null;
    }

    @Override
    public RoundRobinView read(int pos, int size, String tableName, String... columns) throws IOException {
        return null;
    }


    @Override
    public RoundRobinConnector merge(String tableName, int time, RoundRobinView view, MergeType mergeType) throws IOException {
        return null;
    }

    @Override
    public RoundRobinConnector merge(String tableName, RoundRobinView view, MergeType mergeType) throws IOException {
        return null;
    }

    @Override
    public RoundRobinConnector expand(String tableName, String... columns) throws IOException {
        return null;
    }

    @Override
    public RoundRobinConnector createTable(String tableName, String... columns) throws IOException {
        return null;
    }

    @Override
    public RoundRobinConnector dropTable(String... tableNames) throws IOException {
        return null;
    }
}
