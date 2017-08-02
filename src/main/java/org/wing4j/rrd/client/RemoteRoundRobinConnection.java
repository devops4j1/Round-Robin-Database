package org.wing4j.rrd.client;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.*;
import org.wing4j.rrd.net.connector.RoundRobinConnector;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by wing4j on 2017/7/31.
 * 远程访问连接实现
 */
@Data
@ToString
public class RemoteRoundRobinConnection implements RoundRobinConnection{
    volatile RoundRobinDatabase database;
    volatile RoundRobinConnector connector;
    RoundRobinConfig config;

    public RemoteRoundRobinConnection(RoundRobinDatabase database, RoundRobinConnector connector, RoundRobinConfig config) {
        this.database = database;
        this.connector = connector;
        this.config = config;
    }

    @Override
    public RoundRobinConnection lock() {
        return null;
    }

    @Override
    public RoundRobinConnection unlock() {
        return null;
    }

    @Override
    public String[] getHeader() {
        return new String[0];
    }

    @Override
    public RoundRobinResultSet read(String... name) {
        return null;
    }

    @Override
    public boolean contain(String name) {
        return false;
    }

    @Override
    public RoundRobinConnection increase(int sec, String name) {
        return null;
    }

    @Override
    public RoundRobinConnection increase(String name) {
        return null;
    }

    @Override
    public RoundRobinConnection increase(String name, int i) {
        return null;
    }

    @Override
    public RoundRobinConnection increase(int sec, String name, int i) {
        return null;
    }

    @Override
    public RoundRobinView slice(int second, String... name) {
        return null;
    }

    @Override
    public RoundRobinConnection addTrigger(RoundRobinTrigger trigger) {
        return this;
    }

    @Override
    public RoundRobinConnection merge(RoundRobinView view, MergeType mergeType) {
        try {
            this.connector.write(view, view.getTime(), mergeType);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return this;
    }

    @Override
    public RoundRobinConnection merge(RoundRobinView view, int time, MergeType mergeType) {
        try {
            this.connector.write(view, view.getTime(), mergeType);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return this;
    }

    @Override
    public RoundRobinConnection persistent(FormatType formatType, int version) throws IOException {
        return this;
    }

    @Override
    public RoundRobinConnection persistent() throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
