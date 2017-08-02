package org.wing4j.rrd.client;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.*;
import org.wing4j.rrd.net.connector.RoundRobinConnector;
import org.wing4j.rrd.net.connector.impl.AioRoundRobinConnector;
import org.wing4j.rrd.net.connector.impl.BioRoundRobinConnector;
import org.wing4j.rrd.net.connector.impl.NioRoundRobinConnector;

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
