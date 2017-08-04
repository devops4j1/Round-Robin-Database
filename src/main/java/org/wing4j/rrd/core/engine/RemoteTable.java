package org.wing4j.rrd.core.engine;

import org.wing4j.rrd.FormatType;
import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.RoundRobinTrigger;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.core.Table;
import org.wing4j.rrd.core.TableMetadata;

import java.io.IOException;

/**
 * Created by wing4j on 2017/8/4.
 */
public class RemoteTable implements Table{
    @Override
    public TableMetadata getMetadata() {
        return null;
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
    public Table increase(String column) {
        return null;
    }

    @Override
    public Table increase(String column, int val) {
        return null;
    }

    @Override
    public long getSize() {
        return 0;
    }

    @Override
    public Table set(int time, String column, long val) {
        return null;
    }

    @Override
    public long get(int time, String column) {
        return 0;
    }

    @Override
    public RoundRobinView slice(int size, String... columns) {
        return null;
    }

    @Override
    public RoundRobinView slice(int size, int time, String... columns) {
        return null;
    }

    @Override
    public Table expand(String... columns) {
        return null;
    }

    @Override
    public Table merge(RoundRobinView view, int time, MergeType mergeType) {
        return null;
    }

    @Override
    public Table merge(RoundRobinView view, MergeType mergeType) {
        return null;
    }

    @Override
    public Table persistent(FormatType formatType, int version) throws IOException {
        return null;
    }

    @Override
    public Table persistent() throws IOException {
        return null;
    }

    @Override
    public void drop() throws IOException {

    }

    @Override
    public Table registerTrigger(RoundRobinTrigger trigger) {
        return null;
    }
}
