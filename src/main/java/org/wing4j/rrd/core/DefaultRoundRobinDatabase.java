package org.wing4j.rrd.core;

import org.wing4j.rrd.*;
import org.wing4j.rrd.client.RemoteRoundRobinConnection;
import org.wing4j.rrd.core.engine.PersistentTable;
import org.wing4j.rrd.debug.DebugConfig;
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
    //计划任务池
    final ScheduledExecutorService scheduledService;
    //连接池
    final Map<String, RoundRobinConnection> connections = new ConcurrentHashMap();
    //数据库实例
    final static Map<String, RoundRobinDatabase> instances = new ConcurrentHashMap();
    //配置对象
    RoundRobinConfig config;
    //检查连接超时句柄
    Future checkConnectionTimeoutFuture;

    final Map<String, Table> tables = new HashMap<>();

    private DefaultRoundRobinDatabase(String instance, final RoundRobinConfig config) throws IOException {
        this.instance = instance;
        this.config = config;
        this.scheduledService = Executors.newSingleThreadScheduledExecutor();

        this.checkConnectionTimeoutFuture = this.scheduledService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    for (RoundRobinConnection connection : connections.values()) {
                        long time = (System.currentTimeMillis() - connection.getLastActiveTime()) / 1000;
                        if (DebugConfig.DEBUG) {
                            LOGGER.info("sessionId :" + connection.getSessionId() + " last active time " + time + "seconds");
                        }
                        if (time > config.getAutoDisconnectThreshold()) {
                            LOGGER.info("sessionId :" + connection.getSessionId() + " last active time " + time + "seconds ,has timeout! auto disconnect just now!");
                            connection.close();
                            LOGGER.info("sessionId :" + connection.getSessionId() + " has disconnect!");
                        }
                    }
                } catch (IOException e) {
                    LOGGER.warning("schedule check timeout connection happens error!");
                }
            }
        }, config.getAutoDisconnectThreshold(), config.getAutoDisconnectThreshold() / 2, TimeUnit.SECONDS);
        String databasePath = config.getRrdHome() + File.separator + "database" + File.separator + instance;
        File databasePathDir = new File(databasePath);
        if (!databasePathDir.exists()) {
            databasePathDir.mkdirs();
        }
        if (!databasePathDir.exists()) {
            throw new RoundRobinRuntimeException("创建数据库实例工作目录发生错误!");
        }
        File[] tableFiles = databasePathDir.listFiles(new FilenameFilter() {
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
        LOGGER.info("正在加载表[" + instance + "." + table.getMetadata().getName() + "]数据");
        //注册表数据
        tables.put(table.getMetadata().getName(), table);
        LOGGER.info("正在创建表[" + instance + "." + table.getMetadata().getName() + "]任务事件");
        Future future = this.scheduledService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (table.isAutoPersistent()) {
                        if (DebugConfig.DEBUG) {
                            LOGGER.info("" + table.getMetadata().getInstance() + "." + table.getMetadata().getName() + " will persistent!");
                        }
                        table.persistent();
                        if (DebugConfig.DEBUG) {
                            LOGGER.info("" + table.getMetadata().getInstance() + "." + table.getMetadata().getName() + " has persistent!");
                        }
                    }
                } catch (IOException e) {
                    LOGGER.warning("auto persistent " + table.getMetadata().getInstance() + "." + table.getMetadata().getName() + "happens error!");
                }
            }
        }, config.getAutoPersistentPeriodSec(), config.getAutoPersistentPeriodSec(), TimeUnit.SECONDS);
        table.addScheduledFuture(future);
    }

    public static RoundRobinDatabase init(RoundRobinConfig config) throws IOException {
        return init("default", config);
    }

    public static RoundRobinDatabase init(String instance, RoundRobinConfig config) throws IOException {
        RoundRobinDatabase database = instances.get(instance);
        if (database == null) {
            synchronized (instances) {
                database = instances.get(instance);
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
        Table table = new PersistentTable(config.getRrdHome(), instance, tableName, DAY_SECOND, columns);
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
                LOGGER.info("正在卸载表[" + table.getMetadata().getInstance() + "." + tableName + "]");
                tables.remove(tableName);
                LOGGER.info("正在删除[" + table.getMetadata().getInstance() + "." + tableName + "]表数据文件");
                table.drop();
                LOGGER.info("正在结束表[" + table.getMetadata().getInstance() + "." + tableName + "]注册的任务事件");
                for (Future future : table.getScheduledFutures()) {
                    future.cancel(false);
                    table.removeScheduledFutures(future);
                }
            } catch (Exception e) {
                LOGGER.warning("drop table " + table.getMetadata().getInstance() + "." + table.getMetadata().getName() + "happens error!");
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
    public RoundRobinDatabase persistent(String tableName, final FormatType formatType, final int version, int persistentTime) {
        final Table table = getTable(tableName);
        if (persistentTime == 0) {
            try {
                if (DebugConfig.DEBUG) {
                    LOGGER.info("" + table.getMetadata().getInstance() + "." + table.getMetadata().getName() + " will persistent!");
                }
                table.persistent(formatType, version);
                if (DebugConfig.DEBUG) {
                    LOGGER.info("" + table.getMetadata().getInstance() + "." + table.getMetadata().getName() + " has persistent!");
                }
            } catch (IOException e) {
                LOGGER.warning("persistent " + table.getMetadata().getInstance() + "." + table.getMetadata().getName() + "happens error!");
            }
        } else {
            Future future = this.scheduledService.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (DebugConfig.DEBUG) {
                            LOGGER.info("" + table.getMetadata().getInstance() + "." + table.getMetadata().getName() + " will persistent!");
                        }
                        table.persistent(formatType, version);
                        if (DebugConfig.DEBUG) {
                            LOGGER.info("" + table.getMetadata().getInstance() + "." + table.getMetadata().getName() + " has persistent!");
                        }
                    } catch (IOException e) {
                        LOGGER.warning("persistent " + table.getMetadata().getInstance() + "." + table.getMetadata().getName() + "happens error!");
                    }
                }
            }, persistentTime, TimeUnit.SECONDS);
        }
        return this;
    }

    @Override
    public RoundRobinDatabase persistent(final FormatType formatType, final int version, final int persistentTime) {
        for (final Table table : tables.values()) {
            if (persistentTime == 0) {
                persistent(table.getMetadata().getName(), FormatType.BIN, 1, 0);
            } else {
                Future future = this.scheduledService.schedule(new Runnable() {
                    @Override
                    public void run() {
                        if (DebugConfig.DEBUG) {
                            LOGGER.info("" + table.getMetadata().getInstance() + "." + table.getMetadata().getName() + " will persistent!");
                        }
                        persistent(table.getMetadata().getName(), FormatType.BIN, 1, persistentTime);
                        if (DebugConfig.DEBUG) {
                            LOGGER.info("" + table.getMetadata().getInstance() + "." + table.getMetadata().getName() + " has persistent!");
                        }
                    }
                }, persistentTime, TimeUnit.SECONDS);
            }
        }

        return this;
    }

    @Override
    public void close() throws IOException {
        for (RoundRobinConnection connection : connections.values()) {
            connection.close();
            connections.remove(connection);
        }
        checkConnectionTimeoutFuture.cancel(true);
        scheduledService.shutdown();
    }

    @Override
    public void close(RoundRobinConnection connection) throws IOException {
        connections.remove(connection.getSessionId());
    }
}
