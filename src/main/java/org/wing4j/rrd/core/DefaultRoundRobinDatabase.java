package org.wing4j.rrd.core;

import org.wing4j.rrd.RoundRobinConfig;
import org.wing4j.rrd.RoundRobinConnection;
import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.RoundRobinRuntimeException;
import org.wing4j.rrd.client.RemoteRoundRobinConnection;
import org.wing4j.rrd.core.engine.PersistentTable;
import org.wing4j.rrd.net.connector.impl.BioRoundRobinConnector;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/7/28.
 */
public class DefaultRoundRobinDatabase implements RoundRobinDatabase {
    static Logger LOGGER = Logger.getLogger(DefaultRoundRobinDatabase.class.getName());
    //实例名
    String instance;
    ScheduledExecutorService scheduledService = null;

    Map<String, RoundRobinConnection> connections = new ConcurrentHashMap();

    static RoundRobinDatabase database;
    RoundRobinConfig config;

    final Map<String, Table> tables = new HashMap<>();

    private DefaultRoundRobinDatabase(String instance, RoundRobinConfig config) throws IOException {
        this.instance = instance;
        this.config = config;
        this.scheduledService = Executors.newSingleThreadScheduledExecutor();
        String workPath = config.getWorkPath() + File.separator + instance;
        File workPathDir = new File(workPath);
        if (!workPathDir.exists()) {
            workPathDir.mkdirs();
        }
        if (!workPathDir.exists()) {
            //TODO 抛出异常
        }
        File[] tableFiles = workPathDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                String fileName = name.toLowerCase().trim();
                if (fileName.endsWith("rrd")) {
                    return true;
                } else {
                    return false;
                }

            }
        });
        for (File tableFile : tableFiles) {
            openTable(tableFile);
        }
    }

    /**
     * 注册表
     *
     * @param table
     */
    void register(final Table table) {
        LOGGER.info("load table " + instance + "." + table.getMetadata().getName());
        Future future = this.scheduledService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    table.persistent();
                } catch (IOException e) {
                    LOGGER.info("schedule persistent " + table.getMetadata().getInstance() + "." + table.getMetadata().getName() + "happens error!");
                }
            }
        }, 1, 1, TimeUnit.MINUTES);
        table.setScheduledFuture(future);
        //注册后绑定触发器
        tables.put(table.getMetadata().getName(), table);
    }

    public static RoundRobinDatabase init(RoundRobinConfig config) throws IOException {
        return init("default", config);
    }

    public static RoundRobinDatabase init(String instance, RoundRobinConfig config) throws IOException {
        if (database == null) {
            synchronized (RoundRobinDatabase.class) {
                if (database == null) {
                    database = new DefaultRoundRobinDatabase(instance, config);
                }
            }
        }
        return database;
    }


    @Override
    public Map<String, RoundRobinConnection> getConnections() {
        return connections;
    }

    @Override
    public RoundRobinConnection getConnection(String sessionId) {
        RoundRobinConnection connection = connections.get(sessionId);
        if (connection == null) {
            throw new RoundRobinRuntimeException("sessionId [" + sessionId + "] is illegal!");
        }
        return connection;
    }

    @Override
    public RoundRobinConnection open(String address, int port, String username, String password) throws IOException {
        RoundRobinConnection connection = new RemoteRoundRobinConnection(this, new BioRoundRobinConnector(this, config, address, port), username, password);
        connections.put(connection.getSessionId(), connection);
        return connection;
    }

    @Override
    public RoundRobinConnection open() throws IOException {
        //TODO 创建一个计时器，进行数据的异步写入
        RoundRobinConnection connection = new LocalRoundRobinConnection(this);
        connections.put(connection.getSessionId(), connection);
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
        Table table = new PersistentTable(config.getWorkPath(), instance, tableName, DAY_SECOND, columns);
        register(table);
        return this;
    }

    @Override
    public RoundRobinDatabase openTable(File file) throws IOException {
        Table table = new PersistentTable(file);
        register(table);
        return this;
    }

    @Override
    public RoundRobinDatabase dropTable(String... tableNames) throws IOException {
        for (String tableName : tableNames) {
            existTable(tableName, true);
            Table table = tables.get(tableName);
            try {
                table.drop();
                table.getScheduledFuture().cancel(false);
                tables.remove(tableName);
            } catch (Exception e) {
                //TODO 处理删表错误
            }
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
