package org.wing4j.rrd.core;

import org.wing4j.rrd.*;
import org.wing4j.rrd.client.RemoteRoundRobinConnection;
import org.wing4j.rrd.core.engine.PersistentTable;
import org.wing4j.rrd.core.format.bin.v1.RoundRobinFormatBinV1;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wing4j on 2017/7/28.
 */
public class DefaultRoundRobinDatabase implements RoundRobinDatabase {
    Map<RoundRobinConnection, RoundRobinConnection> connections = new ConcurrentHashMap();
    static RoundRobinDatabase database;
    RoundRobinConfig config;

    final Map<String, Table> tables = new HashMap<>();

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


    @Override
    public RoundRobinConnection open(String address, int port) throws IOException {
        RoundRobinConnection connection = new RemoteRoundRobinConnection(this, address, port, config);
        connections.put(connection, connection);
        return connection;
    }

    @Override
    public RoundRobinConnection open() throws IOException {
        //TODO 创建一个计时器，进行数据的异步写入
        RoundRobinConnection connection = new LocalRoundRobinConnection(this);
        connections.put(connection, connection);
        return connection;
    }

    @Override
    public Table getTable(String tableName) {
        existTable(tableName, true);
        return tables.get(tableName);
    }

    @Override
    public RoundRobinDatabase createTable(String tableName, String... columns) throws IOException {
        if (existTable(tableName, false)) {
            throw new RoundRobinRuntimeException(tableName + " is exist!");
        }
        Table table = new PersistentTable(config.getWorkPath(), tableName, DAY_SECOND, columns);
        tables.put(tableName, table);
        return this;
    }

    @Override
    public RoundRobinDatabase createTable(File file) throws IOException {
        Table table = new PersistentTable(file);
        tables.put(table.getMetadata().getName(), table);
        return this;
    }

    @Override
    public RoundRobinDatabase dropTable(String... tableNames) throws IOException {
        for (String tableName : tableNames) {
            existTable(tableName, true);
            Table table = tables.get(tableName);
            table.drop();
        }
        return this;
    }

    /**
     * 检查表存在否
     *
     * @param tableName
     * @param throwException
     * @return
     */
    public boolean existTable(String tableName, boolean throwException) {
        boolean exist = tables.containsKey(tableName);
        if (throwException && !exist) {
            throw new RoundRobinRuntimeException(tableName + " is not exist!");
        }
        return exist;
    }

    @Override
    public List<Table> listTable(String... tableNames) {
        List<Table> result = new ArrayList<>();
        if (tableNames.length == 0) {
            result.addAll(this.tables.values());
        } else {
            for (String tableName : tableNames) {
                Table table = this.tables.get(tableName);
                result.add(table);
            }
        }
        return result;
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
