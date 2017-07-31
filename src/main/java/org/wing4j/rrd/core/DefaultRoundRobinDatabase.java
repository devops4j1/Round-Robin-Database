package org.wing4j.rrd.core;

import org.wing4j.rrd.RoundRobinConfig;
import org.wing4j.rrd.RoundRobinConnection;
import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.RoundRobinFormat;
import org.wing4j.rrd.client.RemoteRoundRobinConnection;
import org.wing4j.rrd.core.format.bin.v1.RoundRobinFormatBinV1;
import org.wing4j.rrd.net.connector.RoundRobinConnector;
import org.wing4j.rrd.net.connector.impl.SocketRoundRobinConnector;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by wing4j on 2017/7/28.
 */
public class DefaultRoundRobinDatabase implements RoundRobinDatabase {
    Set<RoundRobinConnection> connections = new HashSet<>();
    static RoundRobinDatabase database;
    RoundRobinConfig config;
    private DefaultRoundRobinDatabase(RoundRobinConfig config) {
        this.config = config;
    }

    public static RoundRobinDatabase init(RoundRobinConfig config){
        if(database == null){
            synchronized (RoundRobinDatabase.class){
                if(database == null){
                    database = new DefaultRoundRobinDatabase(config);
                }
            }
        }
        return database;
    }

    boolean contain(String[] header, String name) {
        for (String name0 : header) {
            if (name.equals(name0)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public RoundRobinConnection open(String address, int port) throws IOException {
        RoundRobinConnector connector = new SocketRoundRobinConnector(address, port);
        RoundRobinConnection connection = new RemoteRoundRobinConnection(this, connector);
        connections.add(connection);
        return connection;
    }

    @Override
    public RoundRobinConnection open(String fileName, String... names) throws IOException {
        String[] header = new String[names.length + 1];
        if (contain(names, "index")) {
            throw new RuntimeException();
        }
        header[0] = "index";
        for (int i = 0; i < names.length; i++) {
            header[i + 1] = names[i];
        }
        int size = 1 * RoundRobinConnection.DAY_SECOND;
        long[][] data = new long[size][header.length];
        for (int i = 0; i < size; i++) {
            long[] col = new long[header.length];
            col[0] = i;
            for (int j = 0; j < names.length; j++) {
                col[j + 1] = 0L;
            }
            data[i] = col;
        }
        RoundRobinConnection connection = new DefaultRoundRobinConnection(this, header, data, fileName);
        connections.add(connection);
        return connection;
    }
    @Override
    public RoundRobinConnection open(String fileName) throws IOException {
        //创建一个二进制文件
        RoundRobinFormat format = new RoundRobinFormatBinV1();
        format.read(fileName);
        //创建一个计时器，进行数据的异步写入
        RoundRobinConnection connection = new DefaultRoundRobinConnection(this, format.getHeader(), format.getData(), fileName);
        connections.add(connection);
        return connection;
    }

    @Override
    public RoundRobinConfig getConfig() {
        return config;
    }

    @Override
    public void close() throws IOException {
        for (RoundRobinConnection connection : connections){
            connection.close();
            connections.remove(connection);
        }
    }

    @Override
    public void close(RoundRobinConnection connection) throws IOException {
        connections.remove(connection);
    }
}
