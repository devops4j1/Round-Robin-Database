package org.wing4j.rrd.client;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.*;
import org.wing4j.rrd.net.connector.RoundRobinConnector;
import org.wing4j.rrd.net.connector.impl.AioRoundRobinConnector;
import org.wing4j.rrd.net.connector.impl.BioRoundRobinConnector;
import org.wing4j.rrd.net.connector.impl.NioRoundRobinConnector;

import java.io.IOException;
import java.util.Map;

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

    public RemoteRoundRobinConnection(RoundRobinDatabase database, String address, int port, RoundRobinConfig config) {
        this.database = database;
        this.config = config;
        try {
            if(this.config.getConnectorType() == ConnectorType.BIO){
                this.connector = new BioRoundRobinConnector(address, port);
            }else if(this.config.getConnectorType() == ConnectorType.NIO){
                this.connector = new NioRoundRobinConnector(address, port);
            }else if(this.config.getConnectorType() == ConnectorType.AIO){
                this.connector = new AioRoundRobinConnector(address, port);
            }else{
                throw new RoundRobinRuntimeException("不支持的连接器类型");
            }
        } catch (IOException e) {
           //TODO
        }
    }
    @Override
    public String[] getColumns(String tableName) {
        return new String[0];
    }

    @Override
    public boolean contain(String tableName, String column) {
        return false;
    }

    @Override
    public RoundRobinConnection increase(String tableName, String column) {
        return null;
    }

    @Override
    public RoundRobinConnection increase(String tableName, String column, int i) {
        return null;
    }

    @Override
    public RoundRobinView slice(String tableName, int size, String... columns) {
        return null;
    }

    @Override
    public RoundRobinView slice(int size, String... fullNames) {
        return null;
    }

    @Override
    public RoundRobinConnection registerTrigger(String tableName, RoundRobinTrigger trigger) {
        return null;
    }

    @Override
    public RoundRobinConnection merge(String tableName, MergeType mergeType, RoundRobinView view) {
        try {
            this.connector.write(tableName, view.getTime(), view, mergeType);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return this;
    }

    @Override
    public RoundRobinConnection merge(String tableName, MergeType mergeType, RoundRobinView view, Map<String, String> mappings) {
        return null;
    }

    @Override
    public RoundRobinConnection merge(String tableName, MergeType mergeType, int mergePos, RoundRobinView view) {
        return null;
    }

    @Override
    public RoundRobinConnection merge(String tableName, MergeType mergeType, int mergePos, RoundRobinView view, Map<String, String> mappings) {
        return null;
    }

    @Override
    public RoundRobinConnection persistent(FormatType formatType, int version, String... tableNames) throws IOException {
        return null;
    }

    @Override
    public RoundRobinConnection persistent(String... tableNames) throws IOException {
        return null;
    }

    @Override
    public RoundRobinConnection createTable(String tableName, String... columns) throws IOException {
        return null;
    }

    @Override
    public RoundRobinConnection dropTable(String... tableNames) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
