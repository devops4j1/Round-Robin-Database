package org.wing4j.rrd.core;


import org.wing4j.rrd.*;
import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.utils.MessageFormatter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/7/29.
 */
public class LocalRoundRobinConnection implements RoundRobinConnection {
    /**
     * 状态
     */
    int status = Status.NORMAL;
    static Logger LOGGER = Logger.getLogger(LocalRoundRobinConnection.class.getName());
    volatile RoundRobinDatabase database;
    /**
     * 任务执行线程池
     */
    volatile ScheduledExecutorService taskExecutor = Executors.newScheduledThreadPool(20);

    LocalRoundRobinConnection(RoundRobinDatabase database) throws IOException {
        this.database = database;
    }

//    @Override
//    public RoundRobinConnection lock() {
//        if (status != Status.NORMAL) {
//            throw new RuntimeException("数据库未进行冻结");
//        }
//        status = Status.FREEZEN;
//        return this;
//    }
//
//    @Override
//    public RoundRobinConnection unlock() {
//        if (status != Status.FREEZEN) {
//            throw new RuntimeException("数据库未进行冻结");
//        }
//        status = Status.NORMAL;
//        return this;
//    }

    @Override
    public RoundRobinDatabase getDatabase() {
        return database;
    }

    @Override
    public TableMetadata getTableMetadata(String tableName) {
        Table table = database.getTable(tableName);
        return table.getMetadata();
    }

    @Override
    public boolean contain(String tableName, String column) {
        Table table = database.getTable(tableName);
        return table.getMetadata().contain(column);
    }

    @Override
    public long increase(String tableName, String column) {
        Table table = database.getTable(tableName);
        return table.increase(column);
    }

    @Override
    public long increase(String tableName, String column, int i) {
        Table table = database.getTable(tableName);
        return table.increase(column, i);
    }

    @Override
    public long increase(String tableName, String column, int pos, int i) {
        Table table = database.getTable(tableName);
        return table.increase(pos, column, i);
    }

    @Override
    public RoundRobinView slice(String tableName, int pos, int size, String... columns) {
        Table table = database.getTable(tableName);
        return table.slice(pos, size, columns);
    }

    @Override
    public RoundRobinView slice(int size, String... fullNames) {
        return null;
    }

    @Override
    public RoundRobinConnection registerTrigger(String tableName, RoundRobinTrigger trigger) {
        Table table = database.getTable(tableName);
        if (table == null) {
            //TODO
        }
        table.registerTrigger(trigger);
        return this;
    }

    @Override
    public RoundRobinView merge(String tableName, MergeType mergeType, RoundRobinView view) {
        return merge(tableName, mergeType, view.getTime(), view);
    }

    @Override
    public RoundRobinView merge(String tableName, MergeType mergeType, RoundRobinView view, Map<String, String> mappings) {
        return null;
    }

    @Override
    public RoundRobinView merge(String tableName, MergeType mergeType, int mergePos, RoundRobinView view) {
        if (DebugConfig.DEBUG) {
            LOGGER.info(MessageFormatter.format("table:{}", tableName));
            LOGGER.info(MessageFormatter.format("mergeType:{}", mergeType));
            LOGGER.info(MessageFormatter.format("view:{}", Arrays.asList(view.getMetadata().getColumns())));
            LOGGER.info(MessageFormatter.format("pos:{}", view.getTime()));
        }
        if (!database.existTable(tableName, false)) {
            try {
                database.createTable(tableName, view.getMetadata().getColumns());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Table table = database.getTable(tableName);
        return table.merge(view, mergePos, mergeType);
    }

    @Override
    public RoundRobinView merge(String tableName, MergeType mergeType, int mergePos, RoundRobinView view, Map<String, String> mappings) {

        return null;
    }

    @Override
    public RoundRobinConnection persistent(FormatType formatType, int version, String... tableNames) throws IOException {
        for (Table table : database.listTable(tableNames)) {
            table.persistent(formatType, version);
        }
        return this;
    }

    @Override
    public RoundRobinConnection persistent(String... tableNames) throws IOException {
        for (Table table : database.listTable(tableNames)) {
            table.persistent();
        }
        return this;
    }

    @Override
    public Table expand(String tableName, String... columns) {
        return database.getTable(tableName).expand(columns);
    }

    @Override
    public RoundRobinConnection createTable(String tableName, String... columns) throws IOException {
        database.createTable(tableName, columns);
        return this;
    }

    @Override
    public RoundRobinConnection dropTable(String... tableNames) throws IOException {
        database.dropTable(tableNames);
        return this;
    }

    @Override
    public void close() throws IOException {
        if (database.getConfig().isAutoPersistent()) {
            persistent();
            persistent(FormatType.CSV, 1);
        }
        database.close(this);
    }
}
