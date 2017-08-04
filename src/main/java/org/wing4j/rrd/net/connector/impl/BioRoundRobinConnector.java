package org.wing4j.rrd.net.connector.impl;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.core.TableMetadata;
import org.wing4j.rrd.net.connector.RoundRobinConnector;
import org.wing4j.rrd.net.protocol.*;
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
        RoundRobinTableMetadataProtocolV1 protocol = new RoundRobinTableMetadataProtocolV1();
        protocol.setTableName(tableName);
        ByteBuffer buffer = protocol.convert();
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        if(DebugConfig.DEBUG){
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        socket.getOutputStream().write(data);
        InputStream is = socket.getInputStream();
        byte[] data11 = new byte[is.available()];
        is.read(data11);
        System.out.println(new String(data11));
        socket.close();
        //TODO 构建表元信息对象
        return null;
    }

    @Override
    public long increase(String tableName, String column, int i) throws IOException {
        RoundRobinIncreaseProtocolV1 protocol = new RoundRobinIncreaseProtocolV1();
        protocol.setTableName(tableName);
        protocol.setColumn(column);
        protocol.setValue(i);
        ByteBuffer buffer = protocol.convert();
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        if(DebugConfig.DEBUG){
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        socket.getOutputStream().write(data);
        InputStream is = socket.getInputStream();
        byte[] data11 = new byte[is.available()];
        is.read(data11);
        System.out.println(new String(data11));
        socket.close();
        return 0;
    }
    @Override
    public RoundRobinView slice(int pos, int size, String tableName, String... columns) throws IOException {
        RoundRobinSliceProtocolV1 protocol = new RoundRobinSliceProtocolV1();
        protocol.setTableName(tableName);
        protocol.setColumns(columns);
        protocol.setPos(pos);
        protocol.setSize(size);
        ByteBuffer buffer = protocol.convert();
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        if(DebugConfig.DEBUG){
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        socket.getOutputStream().write(data);
        InputStream is = socket.getInputStream();
        byte[] data11 = new byte[is.available()];
        is.read(data11);
        System.out.println(new String(data11));
        socket.close();
        return null;
    }

    @Override
    public RoundRobinConnector merge(String tableName, int time, RoundRobinView view, MergeType mergeType) throws IOException {
        RoundRobinMergeProtocolV1 protocol = new RoundRobinMergeProtocolV1();
        protocol.setTableName(tableName);
        protocol.setColumns(view.getMetadata().getColumns());
        protocol.setData(view.getData());
        protocol.setCurrent(time);
        protocol.setMergeType(mergeType);
        ByteBuffer buffer = protocol.convert();
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        if(DebugConfig.DEBUG){
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
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
        RoundRobinMergeProtocolV1 protocol = new RoundRobinMergeProtocolV1();
        protocol.setData(view.getData());
        protocol.setColumns(view.getMetadata().getColumns());
        protocol.setCurrent(view.getTime());
        protocol.setMergeType(mergeType);
        protocol.setTableName(tableName);
        ByteBuffer buffer = protocol.convert();
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        if(DebugConfig.DEBUG){
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        socket.getOutputStream().write(data);
        InputStream is = socket.getInputStream();
        byte[] data11 = new byte[is.available()];
        is.read(data11);
        System.out.println(new String(data11));
        socket.close();
        return this;
    }

    @Override
    public RoundRobinConnector expand(String tableName, String... columns) throws IOException {
        RoundRobinExpandProtocolV1 protocol = new RoundRobinExpandProtocolV1();
        protocol.setTableName(tableName);
        protocol.setColumns(columns);
        ByteBuffer buffer = protocol.convert();
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        if(DebugConfig.DEBUG){
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        socket.getOutputStream().write(data);
        InputStream is = socket.getInputStream();
        byte[] data11 = new byte[is.available()];
        is.read(data11);
        System.out.println(new String(data11));
        socket.close();
        return this;
    }

    @Override
    public RoundRobinConnector createTable(String tableName, String... columns) throws IOException {
        RoundRobinCreateTableProtocolV1 protocol = new RoundRobinCreateTableProtocolV1();
        protocol.setTableName(tableName);
        protocol.setColumns(columns);
        ByteBuffer buffer = protocol.convert();
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        if(DebugConfig.DEBUG){
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        socket.getOutputStream().write(data);
        InputStream is = socket.getInputStream();
        byte[] data11 = new byte[is.available()];
        is.read(data11);
        System.out.println(new String(data11));
        socket.close();
        return this;
    }

    @Override
    public RoundRobinConnector dropTable(String... tableNames) throws IOException {
        return this;
    }

}
