package org.wing4j.rrd.client;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.*;
import org.wing4j.rrd.core.Table;
import org.wing4j.rrd.core.TableMetadata;
import org.wing4j.rrd.core.engine.RemoteTable;
import org.wing4j.rrd.net.connector.RoundRobinConnector;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by wing4j on 2017/7/31.
 * 远程访问连接实现
 */
@Data
@ToString
public class RemoteRoundRobinConnection implements RoundRobinConnection {
    volatile RoundRobinDatabase database;
    volatile RoundRobinConnector connector;
    String sessionId;

    public RemoteRoundRobinConnection(RoundRobinDatabase database, RoundRobinConnector connector, String username, String password) throws IOException {
        this.database = database;
        this.connector = connector;
        this.sessionId = connector.connect(username, password);
    }

    @Override
    public TableMetadata getTableMetadata(String tableName) {
        Table table = new RemoteTable(tableName, connector);
        TableMetadata metadata = table.getMetadata();
        return metadata;
    }

    @Override
    public boolean contain(String tableName, String column) {
        Table table = new RemoteTable(tableName, connector);
        TableMetadata metadata = table.getMetadata();
        return metadata.contain(column);
    }

    @Override
    public long increase(String tableName, String column) {
        Table table = new RemoteTable(tableName, connector);
        return table.increase(column);
    }

    @Override
    public long increase(String tableName, String column, int i) {
        Table table = new RemoteTable(tableName, connector);
        return table.increase(column, i);
    }

    @Override
    public long increase(String tableName, String column, int pos, int i) {
        Table table = new RemoteTable(tableName, connector);
        return table.increase(pos, column, i);
    }

    @Override
    public RoundRobinView slice(String tableName, int pos,  int size, String... columns) {
        Table table = new RemoteTable(tableName, connector);
        return table.slice(pos, size, columns);
    }

    @Override
    public RoundRobinView slice(int size, String... fullNames) {
        return null;
    }

    @Override
    public RoundRobinConnection registerTrigger(String tableName, RoundRobinTrigger trigger) {
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
        Table table = new RemoteTable(tableName, connector);
        return table.merge(view, mergePos, mergeType);
    }

    @Override
    public RoundRobinView merge(String tableName, MergeType mergeType, int mergePos, RoundRobinView view, Map<String, String> mappings) {
        return null;
    }

    @Override
    public RoundRobinConnection persistent(FormatType formatType, int version, String... tableNames) throws IOException {
        return this;
    }

    @Override
    public RoundRobinConnection persistent(String... tableNames) throws IOException {
        return this;
    }

    @Override
    public RoundRobinConnection persistent(int persistentTime, String... tableNames) throws IOException {
        return null;
    }

    @Override
    public Table expand(String tableName, String... columns) {
        return null;
    }

    @Override
    public RoundRobinConnection createTable(String tableName, String... columns) throws IOException {
        this.connector.createTable(tableName, columns);
        return this;
    }

    @Override
    public RoundRobinConnection dropTable(String... tableNames) throws IOException {
        for (String tableName : tableNames) {
            Table table = new RemoteTable(tableName, connector);
            table.drop();
        }
        return this;
    }

    @Override
    public int execute(String sql) throws SQLException {
        return 0;
    }

    @Override
    public RoundRobinView executeQuery(String sql) throws SQLException {
        return null;
    }

    @Override
    public void close() throws IOException {
        connector.disConnect(sessionId);
        database.close(this);
    }

    @Override
    public long getLastActiveTime() {
        return 0;
    }
}
