package org.wing4j.rrd.core.engine;

import org.wing4j.rrd.*;
import org.wing4j.rrd.core.Table;
import org.wing4j.rrd.core.TableMetadata;
import org.wing4j.rrd.net.connector.RoundRobinConnector;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * Created by wing4j on 2017/8/4.
 */
public class RemoteTable implements Table {
    Set<Future> futures = new HashSet<>();
    String tableName;
    RoundRobinConnector connector;

    public RemoteTable(String tableName, RoundRobinConnector connector) {
        this.tableName = tableName;
        this.connector = connector;
    }

    @Override
    public boolean isAutoPersistent() {
        return false;
    }

    @Override
    public Table addScheduledFuture(Future future) {
        futures.add(future);
        return this;
    }

    @Override
    public Future[] getScheduledFutures() {
        return futures.toArray(new Future[futures.size()]);
    }

    @Override
    public Table removeScheduledFutures(Future future) {
        future.cancel(true);
        futures.remove(future);
        return this;
    }

    @Override
    public TableMetadata getMetadata() {
        try {
            return connector.getTableMetadata(tableName);
        } catch (Exception e) {
            throw new RoundRobinRuntimeException("", e);
        }
    }

    @Override
    public Table lock() {
        return null;
    }

    @Override
    public Table unlock() {
        return null;
    }

    @Override
    public long increase(String column) {
        try {
            return connector.increase(tableName, column, -1, 1);
        } catch (Exception e) {
            throw new RoundRobinRuntimeException("", e);
        }
    }

    @Override
    public long increase(String column, int val) {
        try {
            return connector.increase(tableName, column, -1, val);
        } catch (Exception e) {
            throw new RoundRobinRuntimeException("", e);
        }
    }


    @Override
    public long increase(int pos, String column, int val) {
        try {
            return connector.increase(tableName, column, pos, val);
        } catch (Exception e) {
            throw new RoundRobinRuntimeException("", e);
        }
    }

    @Override
    public int getSize() {
        try {
            return getMetadata().getDataSize();
        } catch (Exception e) {
            throw new RoundRobinRuntimeException("", e);
        }
    }

    @Override
    public long set(int time, String column, long val) {
        return 0;
    }

    @Override
    public long get(int time, String column) {
        return 0;
    }

    @Override
    public RoundRobinView slice(int size, String... columns) {
        try {
            return connector.slice(tableName, -1, size, columns);
        } catch (Exception e) {
            throw new RoundRobinRuntimeException("", e);
        }
    }

    @Override
    public RoundRobinView slice(int size, int time, String... columns) {
        try {
            return connector.slice(tableName, time, size, columns);
        } catch (Exception e) {
            throw new RoundRobinRuntimeException("", e);
        }
    }

    @Override
    public Table expand(String... columns) {
        try {
            connector.expand(tableName, columns);
        } catch (Exception e) {

        }
        return this;
    }

    @Override
    public RoundRobinView merge(RoundRobinView view, int time, MergeType mergeType) {
        try {
            return connector.merge(tableName, mergeType, view, time);
        } catch (Exception e) {
            throw new RoundRobinRuntimeException("xxx");
        }
    }

    @Override
    public RoundRobinView merge(RoundRobinView view, MergeType mergeType) {
        return merge(view, view.getTime(), mergeType);
    }

    @Override
    public Table persistent(FormatType formatType, int version) throws IOException {
        System.out.println("尉氏县");
        return this;
    }

    @Override
    public Table persistent() throws IOException {
        System.out.println("尉氏县");
        return this;
    }

    @Override
    public void drop() throws IOException {
        try {
            connector.dropTable(tableName);
        } catch (Exception e) {

        }
    }

    @Override
    public Table registerTrigger(RoundRobinTrigger trigger) {
        return null;
    }
}
