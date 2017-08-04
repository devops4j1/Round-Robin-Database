package org.wing4j.rrd.net.connector.impl;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.RoundRobinFormat;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.core.TableMetadata;
import org.wing4j.rrd.net.connector.RoundRobinConnector;
import org.wing4j.rrd.core.format.net.RoundRobinFormatNetworkV1;
import org.wing4j.rrd.utils.HexUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * Created by wing4j on 2017/7/31.
 */
@Data
@ToString
public class BioRoundRobinConnector implements RoundRobinConnector {
    String address;
    int port;
    Socket socket;

    public BioRoundRobinConnector(String address, int port) throws IOException {
        this.address = address;
        this.port = port;
        this.socket = new Socket(address, port);
    }

    @Override
    public TableMetadata getTableMetadata(String tableName) throws IOException {
        return null;
    }

    @Override
    public int getDataSize(String tableName) throws IOException {
        return 0;
    }

    @Override
    public long increase(String tableName, String column, int i) throws IOException {
        return 0;
    }

    @Override
    public RoundRobinView read(int size, String tableName, String... columns) throws IOException {
        return null;
    }

    @Override
    public RoundRobinView read(int pos, int size, String tableName, String... columns) throws IOException {
        return null;
    }

    @Override
    public RoundRobinConnector merge(String tableName, int time, RoundRobinView view, MergeType mergeType) throws IOException {
        RoundRobinFormat format = new RoundRobinFormatNetworkV1(mergeType, time, tableName, view);
        ByteBuffer buffer = format.write();
        byte[] data = new byte[buffer.position()];
        buffer.flip();
        buffer.get(data);
        System.out.println(data.length);
        System.out.println(HexUtils.toDisplayString(data));
        socket.getOutputStream().write(data);
        InputStream is = socket.getInputStream();
        byte[] data11 = new byte[is.available()];
        is.read(data11);
        System.out.println(new String(data11));
        socket.close();
        return this;
    }

    @Override
    public RoundRobinConnector merge(String tableName, RoundRobinView view, MergeType mergeType) throws IOException {
        return null;
    }

    @Override
    public RoundRobinConnector expand(String tableName, String... columns) throws IOException {
        return null;
    }

    @Override
    public RoundRobinConnector createTable(String tableName, String... columns) throws IOException {
        return null;
    }

    @Override
    public RoundRobinConnector dropTable(String... tableNames) throws IOException {
        return null;
    }

}
