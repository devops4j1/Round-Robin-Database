package org.wing4j.rrd.core;

import org.wing4j.rrd.RoundRobinConfig;
import org.wing4j.rrd.RoundRobinConnection;
import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.RoundRobinFormat;
import org.wing4j.rrd.client.RemoteRoundRobinConnection;
import org.wing4j.rrd.core.format.bin.v1.RoundRobinFormatBinV1;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wing4j on 2017/7/28.
 */
public class DefaultRoundRobinDatabase implements RoundRobinDatabase {
    Map<String, RoundRobinConnection> connections = new ConcurrentHashMap<>();
    static RoundRobinDatabase database;
    RoundRobinConfig config;

    private DefaultRoundRobinDatabase(RoundRobinConfig config) {
        this.config = config;
    }

    public static RoundRobinDatabase init(RoundRobinConfig config) {
        if (database == null) {
            synchronized (RoundRobinDatabase.class) {
                if (database == null) {
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
    public RoundRobinConnection create(String fileName) throws IOException {
        File databaseFile = new File(fileName);
        if (databaseFile.exists()) {
            return open(fileName);
        } else {
            return open(fileName, new String[0]);
        }
    }

    @Override
    public RoundRobinConnection open(String address, int port) throws IOException {
        RoundRobinConnection connection = new RemoteRoundRobinConnection(this, address, port, config);
        String key = "rrd://" + address + ":" + port;
        connections.put(key, connection);
        return connection;
    }

    @Override
    public RoundRobinConnection open(String fileName, String... names) throws IOException {
        int size = 1 * RoundRobinConnection.DAY_SECOND;
        long[][] data = new long[size][names.length];
        for (int i = 0; i < size; i++) {
            if (names.length == 0) {
                data[i] = new long[0];
            } else {
                long[] col = new long[names.length];
                for (int j = 0; j < names.length; j++) {
                    col[j] = 0L;
                }
                data[i] = col;
            }
        }
        RoundRobinConnection connection = new DefaultRoundRobinConnection(this, names, data, fileName);
        String key = "file://" + fileName;
        connections.put(key, connection);
        return connection;
    }

    @Override
    public RoundRobinConnection open(String fileName) throws IOException {
        //创建一个二进制文件
        RoundRobinFormat format = new RoundRobinFormatBinV1();
        format.read(fileName);
        //创建一个计时器，进行数据的异步写入
        RoundRobinConnection connection = new DefaultRoundRobinConnection(this, format.getHeader(), format.getData(), fileName);
        String key = "file://" + fileName;
        connections.put(key, connection);
        return connection;
    }

    @Override
    public RoundRobinConfig getConfig() {
        return config;
    }

    @Override
    public void close() throws IOException {
        for (RoundRobinConnection connection : connections.values()) {
            connection.close();
            connections.remove(connection);
        }
    }

    @Override
    public void close(RoundRobinConnection connection) throws IOException {
        connections.remove(connection);
    }
}
