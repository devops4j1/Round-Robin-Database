package org.wing4j.rrd.core;


import org.wing4j.rrd.*;
import org.wing4j.rrd.debug.DebugConfig;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by wing4j on 2017/7/29.
 */
public class LocalRoundRobinConnection implements RoundRobinConnection {
    /**
     * 状态
     */
    int status = Status.NORMAL;
    volatile RoundRobinDatabase database;
    /**
     * 任务执行线程池
     */
    volatile ScheduledExecutorService taskExecutor = Executors.newScheduledThreadPool(20);

    LocalRoundRobinConnection(RoundRobinDatabase database) throws IOException {
        this.database = database;
        String workPath = database.getConfig().getWorkPath();
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
            database.openTable(tableFile);
        }
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
    public String[] getColumns(String tableName) {
        Table table = database.getTable(tableName);
        return table.getMetadata().getColumns();
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
    public RoundRobinView slice(String tableName, int size, String... columns) {
        Table table = database.getTable(tableName);
        return table.slice(size, columns);
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
    public RoundRobinConnection merge(String tableName, MergeType mergeType, RoundRobinView view) {
        return merge(tableName, mergeType, view.getTime(), view);
    }

    @Override
    public RoundRobinConnection merge(String tableName, MergeType mergeType, RoundRobinView view, Map<String, String> mappings) {
        return null;
    }

    @Override
    public RoundRobinConnection merge(String tableName, MergeType mergeType, int mergePos, RoundRobinView view) {
        if (DebugConfig.DEBUG) {
            System.out.println("table:" + tableName);
            System.out.println("mergeType:" + mergeType);
            System.out.println("view:" + Arrays.asList(view.getMetadata().getColumns()));
            System.out.println("time:" + view.getTime());
        }
        if (!database.existTable(tableName, false)) {
            try {
                database.createTable(tableName, view.getMetadata().getColumns());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Table table = database.getTable(tableName);
        table.merge(view, mergePos, mergeType);
        return this;
    }

    @Override
    public RoundRobinConnection merge(String tableName, MergeType mergeType, int mergePos, RoundRobinView view, Map<String, String> mappings) {

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
        }
        database.close(this);
    }
}
