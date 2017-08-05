package org.wing4j.rrd.net.connector.impl;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.FormatType;
import org.wing4j.rrd.RoundRobinRuntimeException;
import org.wing4j.rrd.core.TableStatus;
import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.core.TableMetadata;
import org.wing4j.rrd.net.connector.RoundRobinConnector;
import org.wing4j.rrd.net.protocol.*;
import org.wing4j.rrd.utils.HexUtils;
import org.wing4j.rrd.utils.MessageUtils;

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
        if (DebugConfig.DEBUG) {
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        socket.getOutputStream().write(data);
        InputStream is = socket.getInputStream();
        byte[] sizeLenBytes = new byte[4];
        is.read(sizeLenBytes);
        int len =  MessageUtils.bytes2int(sizeLenBytes);
        if(is.available() < len - 4){
            System.out.println(HexUtils.toDisplayString(sizeLenBytes));
            throw new RoundRobinRuntimeException("无效报文");
        }
        byte[] dataBytes = new byte[len];
        is.read(dataBytes);
        socket.close();
        buffer = ByteBuffer.wrap(dataBytes);
        ProtocolType protocolType = ProtocolType.valueOfCode(buffer.getInt());
        int version = buffer.getInt();
        MessageType messageType = MessageType.valueOfCode(buffer.getInt());
        if(protocolType == ProtocolType.TABLE_METADATA && version == 1 && messageType == MessageType.RESPONSE){
            protocol.convert(buffer);
            //构建表元信息对象
            TableMetadata metadata = new TableMetadata(null, FormatType.BIN, protocol.getTableName(), protocol.getColumns(), protocol.getDataSize(), protocol.getStatus());
            return metadata;
        }else{
            System.out.println(HexUtils.toDisplayString(dataBytes));
            throw new RoundRobinRuntimeException("无效的应答");
        }

    }

    @Override
    public long increase(String tableName, String column, int pos, int i) throws IOException {
        RoundRobinIncreaseProtocolV1 protocol = new RoundRobinIncreaseProtocolV1();
        protocol.setTableName(tableName);
        protocol.setColumn(column);
        protocol.setPos(pos);
        protocol.setValue(i);
        ByteBuffer buffer = protocol.convert();
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        if (DebugConfig.DEBUG) {
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        socket.getOutputStream().write(data);
        InputStream is = socket.getInputStream();
        byte[] sizeLenBytes = new byte[4];
        is.read(sizeLenBytes);
        int len =  MessageUtils.bytes2int(sizeLenBytes);
        if(is.available() < len - 4){
            System.out.println(HexUtils.toDisplayString(sizeLenBytes));
            throw new RoundRobinRuntimeException("无效报文");
        }
        byte[] dataBytes = new byte[len];
        is.read(dataBytes);
        socket.close();
        buffer = ByteBuffer.wrap(dataBytes);
        ProtocolType protocolType = ProtocolType.valueOfCode(buffer.getInt());
        int version = buffer.getInt();
        MessageType messageType = MessageType.valueOfCode(buffer.getInt());
        if(protocolType == ProtocolType.INCREASE && version == 1 && messageType == MessageType.RESPONSE){
            protocol.convert(buffer);
            return protocol.getNewValue();
        }else{
            System.out.println(HexUtils.toDisplayString(dataBytes));
            throw new RoundRobinRuntimeException("无效的应答");
        }
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
        if (DebugConfig.DEBUG) {
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        socket.getOutputStream().write(data);
        InputStream is = socket.getInputStream();
        byte[] sizeLenBytes = new byte[4];
        is.read(sizeLenBytes);
        int len =  MessageUtils.bytes2int(sizeLenBytes);
        if(is.available() < len - 4){
            System.out.println(HexUtils.toDisplayString(sizeLenBytes));
            throw new RoundRobinRuntimeException("无效报文");
        }
        byte[] dataBytes = new byte[len];
        is.read(dataBytes);
        socket.close();
        buffer = ByteBuffer.wrap(dataBytes);
        ProtocolType protocolType = ProtocolType.valueOfCode(buffer.getInt());
        int version = buffer.getInt();
        MessageType messageType = MessageType.valueOfCode(buffer.getInt());
        if(protocolType == ProtocolType.SLICE && version == 1 && messageType == MessageType.RESPONSE){
            protocol.convert(buffer);
            RoundRobinView view = new RoundRobinView(protocol.getColumns(), protocol.getPos(), protocol.getData());
            return view;
        }else{
            System.out.println(HexUtils.toDisplayString(dataBytes));
            throw new RoundRobinRuntimeException("无效的应答");
        }
    }

    @Override
    public RoundRobinView merge(String tableName, int time, RoundRobinView view, MergeType mergeType) throws IOException {
        RoundRobinMergeProtocolV1 protocol = new RoundRobinMergeProtocolV1();
        protocol.setTableName(tableName);
        protocol.setColumns(view.getMetadata().getColumns());
        protocol.setSize(view.getData().length);
        protocol.setData(view.getData());
        protocol.setPos(time);
        protocol.setMergeType(mergeType);
        ByteBuffer buffer = protocol.convert();
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        if (DebugConfig.DEBUG) {
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        socket.getOutputStream().write(data);
        InputStream is = socket.getInputStream();
        byte[] sizeLenBytes = new byte[4];
        is.read(sizeLenBytes);
        int len =  MessageUtils.bytes2int(sizeLenBytes);
        if(is.available() < len - 4){
            System.out.println(HexUtils.toDisplayString(sizeLenBytes));
            throw new RoundRobinRuntimeException("无效报文");
        }
        byte[] dataBytes = new byte[len];
        is.read(dataBytes);
        socket.close();
        buffer = ByteBuffer.wrap(dataBytes);
        ProtocolType protocolType = ProtocolType.valueOfCode(buffer.getInt());
        int version = buffer.getInt();
        MessageType messageType = MessageType.valueOfCode(buffer.getInt());
        if(protocolType == ProtocolType.MERGE && version == 1 && messageType == MessageType.RESPONSE){
            protocol.convert(buffer);
            //构建表元信息对象
            return new RoundRobinView(protocol.getColumns(), protocol.getPos(), protocol.getData());
        }else{
            System.out.println(HexUtils.toDisplayString(dataBytes));
            throw new RoundRobinRuntimeException("无效的应答");
        }
    }


    @Override
    public TableMetadata expand(String tableName, String... columns) throws IOException {
        RoundRobinExpandProtocolV1 protocol = new RoundRobinExpandProtocolV1();
        protocol.setTableName(tableName);
        protocol.setColumns(columns);
        ByteBuffer buffer = protocol.convert();
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        if (DebugConfig.DEBUG) {
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        socket.getOutputStream().write(data);
        InputStream is = socket.getInputStream();
        byte[] sizeLenBytes = new byte[4];
        is.read(sizeLenBytes);
        int len =  MessageUtils.bytes2int(sizeLenBytes);
        if(is.available() < len - 4){
            System.out.println(HexUtils.toDisplayString(sizeLenBytes));
            throw new RoundRobinRuntimeException("无效报文");
        }
        byte[] dataBytes = new byte[len];
        is.read(dataBytes);
        socket.close();
        buffer = ByteBuffer.wrap(dataBytes);
        ProtocolType protocolType = ProtocolType.valueOfCode(buffer.getInt());
        int version = buffer.getInt();
        MessageType messageType = MessageType.valueOfCode(buffer.getInt());
        if(protocolType == ProtocolType.EXPAND && version == 1 && messageType == MessageType.RESPONSE){
            protocol.convert(buffer);
            //构建表元信息对象
            TableMetadata metadata = new TableMetadata(null, FormatType.BIN, protocol.getTableName(), protocol.getColumns(), 0, TableStatus.UNKNOWN);
            return metadata;
        }else{
            System.out.println(HexUtils.toDisplayString(dataBytes));
            throw new RoundRobinRuntimeException("无效的应答");
        }
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
        if (DebugConfig.DEBUG) {
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        socket.getOutputStream().write(data);
        InputStream is = socket.getInputStream();
        socket.close();
        return this;
    }

    @Override
    public RoundRobinConnector dropTable(String... tableNames) throws IOException {
        return this;
    }

}
