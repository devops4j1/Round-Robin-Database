package org.wing4j.rrd.net.connector.impl;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.*;
import org.wing4j.rrd.core.TableMetadata;
import org.wing4j.rrd.core.TableStatus;
import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.net.connector.RoundRobinConnector;
import org.wing4j.rrd.net.protocol.*;
import org.wing4j.rrd.utils.HexUtils;
import org.wing4j.rrd.utils.MessageUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.sql.SQLException;

/**
 * Created by wing4j on 2017/7/31.
 */
@Data
@ToString
public class BioRoundRobinConnector implements RoundRobinConnector {
    String address;
    int port;
    Socket socket;
    RoundRobinDatabase database;
    RoundRobinConfig config;
    String sessionId;

    public BioRoundRobinConnector(RoundRobinDatabase database, RoundRobinConfig config, String address, int port) throws IOException {
        this.address = address;
        this.port = port;
        this.database = database;
        this.config = config;
    }

    void ifCloseThenReopenSocket() throws IOException {
        if (this.socket == null || this.socket.isClosed()) {
            this.socket = new Socket(address, port);
        }
    }

    @Override
    public String connect(String username, String password) throws IOException {
        ifCloseThenReopenSocket();
        RoundRobinConnectProtocolV1 protocol = new RoundRobinConnectProtocolV1();
        protocol.setUsername(username);
        protocol.setPassword(password);
        ByteBuffer buffer = protocol.convert();
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        if (DebugConfig.DEBUG) {
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        OutputStream os = socket.getOutputStream();
        try {
            os.write(data);
        } catch (Exception e) {
            os.close();
            socket.close();
            throw new RoundRobinRuntimeException("发送数据发生异常");
        }
        InputStream is = socket.getInputStream();
        byte[] sizeLenBytes = new byte[4];
        byte[] dataBytes = new byte[0];
        try {
            is.read(sizeLenBytes);
            int len = MessageUtils.bytes2int(sizeLenBytes);
            if (is.available() < len - 4) {
                System.out.println(HexUtils.toDisplayString(sizeLenBytes));
                throw new RoundRobinRuntimeException("无效报文");
            }
            dataBytes = new byte[len];
            is.read(dataBytes);
        } catch (Exception e) {
            socket.close();
        } finally {
            os.flush();
            is.close();
            os.close();
        }
        buffer = ByteBuffer.wrap(dataBytes);
        ProtocolType protocolType = ProtocolType.valueOfCode(buffer.getInt());
        int version = buffer.getInt();
        MessageType messageType = MessageType.valueOfCode(buffer.getInt());
        if (protocolType == ProtocolType.CONNECT && version == 1 && messageType == MessageType.RESPONSE) {
            protocol.convert(buffer);
            if (RspCode.valueOfCode(protocol.getCode()) == RspCode.SUCCESS) {
                //返回自增后的值
                this.sessionId = protocol.getSessionId();
                return this.sessionId;
            } else {
                throw new RoundRobinRuntimeException(protocol.getCode() + ":" + protocol.getDesc());
            }
        } else {
            System.out.println(HexUtils.toDisplayString(dataBytes));
            throw new RoundRobinRuntimeException("无效的应答");
        }
    }

    @Override
    public void disconnect(String sessionId) throws IOException {
        ifCloseThenReopenSocket();
        RoundRobinDisConnectProtocolV1 protocol = new RoundRobinDisConnectProtocolV1();
        protocol.setSessionId(sessionId);
        ByteBuffer buffer = protocol.convert();
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        if (DebugConfig.DEBUG) {
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        OutputStream os = socket.getOutputStream();
        try {
            os.write(data);
        } catch (Exception e) {
            os.flush();
            socket.close();
            throw new RoundRobinRuntimeException("发送数据发生异常");
        }
        InputStream is = socket.getInputStream();
        byte[] sizeLenBytes = new byte[4];
        byte[] dataBytes = new byte[0];
        try {
            is.read(sizeLenBytes);
            int len = MessageUtils.bytes2int(sizeLenBytes);
            if (is.available() < len - 4) {
                System.out.println(HexUtils.toDisplayString(sizeLenBytes));
                throw new RoundRobinRuntimeException("无效报文");
            }
            dataBytes = new byte[len];
            is.read(dataBytes);
        } catch (Exception e) {
            socket.close();
        } finally {
            os.flush();
            is.close();
            os.close();
        }
        buffer = ByteBuffer.wrap(dataBytes);
        ProtocolType protocolType = ProtocolType.valueOfCode(buffer.getInt());
        int version = buffer.getInt();
        MessageType messageType = MessageType.valueOfCode(buffer.getInt());
        if (protocolType == ProtocolType.DIS_CONNECT && version == 1 && messageType == MessageType.RESPONSE) {
            protocol.convert(buffer);
            if (RspCode.valueOfCode(protocol.getCode()) == RspCode.SUCCESS) {
                //返回自增后的值
            } else {
                throw new RoundRobinRuntimeException(protocol.getCode() + ":" + protocol.getDesc());
            }
        } else {
            System.out.println(HexUtils.toDisplayString(dataBytes));
            throw new RoundRobinRuntimeException("无效的应答");
        }
    }

    @Override
    public TableMetadata getTableMetadata(String tableName) throws IOException {
        ifCloseThenReopenSocket();
        RoundRobinTableMetadataProtocolV1 protocol = new RoundRobinTableMetadataProtocolV1();
        protocol.setTableName(tableName);
        protocol.setSessionId(sessionId);
        ByteBuffer buffer = protocol.convert();
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        if (DebugConfig.DEBUG) {
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        OutputStream os = socket.getOutputStream();
        try {
            os.write(data);
        } catch (Exception e) {
            os.flush();
            os.close();
            socket.close();
            throw new RoundRobinRuntimeException("发送数据发生异常");
        }
        InputStream is = socket.getInputStream();
        byte[] sizeLenBytes = new byte[4];
        byte[] dataBytes = new byte[0];
        try {
            is.read(sizeLenBytes);
            int len = MessageUtils.bytes2int(sizeLenBytes);
            if (is.available() < len - 4) {
                System.out.println(HexUtils.toDisplayString(sizeLenBytes));
                throw new RoundRobinRuntimeException("无效报文");
            }
            dataBytes = new byte[len];
            is.read(dataBytes);
        } catch (Exception e) {
            socket.close();
        } finally {
            os.flush();
            is.close();
            os.close();
        }
        buffer = ByteBuffer.wrap(dataBytes);
        ProtocolType protocolType = ProtocolType.valueOfCode(buffer.getInt());
        int version = buffer.getInt();
        MessageType messageType = MessageType.valueOfCode(buffer.getInt());
        if (protocolType == ProtocolType.TABLE_METADATA && version == 1 && messageType == MessageType.RESPONSE) {
            protocol.convert(buffer);
            if (RspCode.valueOfCode(protocol.getCode()) == RspCode.SUCCESS) {
                //构建表元信息对象
                TableMetadata metadata = new TableMetadata(null, FormatType.BIN, protocol.getInstance(), protocol.getTableName(), protocol.getColumns(), 0, TableStatus.UNKNOWN);
                return metadata;
            } else {
                throw new RoundRobinRuntimeException(protocol.getCode() + ":" + protocol.getDesc());
            }
        } else {
            System.out.println(HexUtils.toDisplayString(dataBytes));
            throw new RoundRobinRuntimeException("无效的应答");
        }

    }

    @Override
    public long increase(String tableName, String column, int pos, int i) throws IOException {
        ifCloseThenReopenSocket();
        RoundRobinIncreaseProtocolV1 protocol = new RoundRobinIncreaseProtocolV1();
        protocol.setSessionId(sessionId);
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
        OutputStream os = socket.getOutputStream();
        try {
            os.write(data);
        } catch (Exception e) {
            os.close();
            socket.close();
            throw new RoundRobinRuntimeException("发送数据发生异常");
        }
        InputStream is = socket.getInputStream();
        byte[] sizeLenBytes = new byte[4];
        byte[] dataBytes = new byte[0];
        try {
            is.read(sizeLenBytes);
            int len = MessageUtils.bytes2int(sizeLenBytes);
            if (is.available() < len - 4) {
                System.out.println(HexUtils.toDisplayString(sizeLenBytes));
                throw new RoundRobinRuntimeException("无效报文");
            }
            dataBytes = new byte[len];
            is.read(dataBytes);
        } catch (Exception e) {
            socket.close();
        } finally {
            os.flush();
            is.close();
            os.close();
        }
        buffer = ByteBuffer.wrap(dataBytes);
        ProtocolType protocolType = ProtocolType.valueOfCode(buffer.getInt());
        int version = buffer.getInt();
        MessageType messageType = MessageType.valueOfCode(buffer.getInt());
        if (protocolType == ProtocolType.INCREASE && version == 1 && messageType == MessageType.RESPONSE) {
            protocol.convert(buffer);
            if (RspCode.valueOfCode(protocol.getCode()) == RspCode.SUCCESS) {
                //返回自增后的值
                return protocol.getNewValue();
            } else {
                throw new RoundRobinRuntimeException(protocol.getCode() + ":" + protocol.getDesc());
            }
        } else {
            System.out.println(HexUtils.toDisplayString(dataBytes));
            throw new RoundRobinRuntimeException("无效的应答");
        }
    }

    @Override
    public long set(String tableName, String column, int pos, long val) throws IOException {
        ifCloseThenReopenSocket();
        RoundRobinSetProtocolV1 protocol = new RoundRobinSetProtocolV1();
        protocol.setSessionId(sessionId);
        protocol.setTableName(tableName);
        protocol.setColumn(column);
        protocol.setPos(pos);
        protocol.setValue(val);
        ByteBuffer buffer = protocol.convert();
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        if (DebugConfig.DEBUG) {
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        OutputStream os = socket.getOutputStream();
        try {
            os.write(data);
        } catch (Exception e) {
            os.close();
            socket.close();
            throw new RoundRobinRuntimeException("发送数据发生异常");
        }
        InputStream is = socket.getInputStream();
        byte[] sizeLenBytes = new byte[4];
        byte[] dataBytes = new byte[0];
        try {
            is.read(sizeLenBytes);
            int len = MessageUtils.bytes2int(sizeLenBytes);
            if (is.available() < len - 4) {
                System.out.println(HexUtils.toDisplayString(sizeLenBytes));
                throw new RoundRobinRuntimeException("无效报文");
            }
            dataBytes = new byte[len];
            is.read(dataBytes);
        } catch (Exception e) {
            socket.close();
        } finally {
            os.flush();
            is.close();
            os.close();
        }
        buffer = ByteBuffer.wrap(dataBytes);
        ProtocolType protocolType = ProtocolType.valueOfCode(buffer.getInt());
        int version = buffer.getInt();
        MessageType messageType = MessageType.valueOfCode(buffer.getInt());
        if (protocolType == ProtocolType.SET && version == 1 && messageType == MessageType.RESPONSE) {
            protocol.convert(buffer);
            if (RspCode.valueOfCode(protocol.getCode()) == RspCode.SUCCESS) {
                //返回自增后的值
                return protocol.getNewValue();
            } else {
                throw new RoundRobinRuntimeException(protocol.getCode() + ":" + protocol.getDesc());
            }
        } else {
            System.out.println(HexUtils.toDisplayString(dataBytes));
            throw new RoundRobinRuntimeException("无效的应答");
        }
    }

    @Override
    public long get(String tableName, String column, int pos) throws IOException {
        ifCloseThenReopenSocket();
        RoundRobinGetProtocolV1 protocol = new RoundRobinGetProtocolV1();
        protocol.setSessionId(sessionId);
        protocol.setTableName(tableName);
        protocol.setColumn(column);
        protocol.setPos(pos);
        ByteBuffer buffer = protocol.convert();
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        if (DebugConfig.DEBUG) {
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        OutputStream os = socket.getOutputStream();
        try {
            os.write(data);
        } catch (Exception e) {
            os.close();
            socket.close();
            throw new RoundRobinRuntimeException("发送数据发生异常");
        }
        InputStream is = socket.getInputStream();
        byte[] sizeLenBytes = new byte[4];
        byte[] dataBytes = new byte[0];
        try {
            is.read(sizeLenBytes);
            int len = MessageUtils.bytes2int(sizeLenBytes);
            if (is.available() < len - 4) {
                System.out.println(HexUtils.toDisplayString(sizeLenBytes));
                throw new RoundRobinRuntimeException("无效报文");
            }
            dataBytes = new byte[len];
            is.read(dataBytes);
        } catch (Exception e) {
            socket.close();
        } finally {
            os.flush();
            is.close();
            os.close();
        }
        buffer = ByteBuffer.wrap(dataBytes);
        ProtocolType protocolType = ProtocolType.valueOfCode(buffer.getInt());
        int version = buffer.getInt();
        MessageType messageType = MessageType.valueOfCode(buffer.getInt());
        if (protocolType == ProtocolType.GET && version == 1 && messageType == MessageType.RESPONSE) {
            protocol.convert(buffer);
            if (RspCode.valueOfCode(protocol.getCode()) == RspCode.SUCCESS) {
                //返回自增后的值
                return protocol.getValue();
            } else {
                throw new RoundRobinRuntimeException(protocol.getCode() + ":" + protocol.getDesc());
            }
        } else {
            System.out.println(HexUtils.toDisplayString(dataBytes));
            throw new RoundRobinRuntimeException("无效的应答");
        }
    }

    @Override
    public RoundRobinView slice(String tableName, int pos, int size, String... columns) throws IOException {
        ifCloseThenReopenSocket();
        RoundRobinSliceProtocolV1 protocol = new RoundRobinSliceProtocolV1();
        protocol.setSessionId(sessionId);
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
        OutputStream os = socket.getOutputStream();
        try {
            os.write(data);
        } catch (Exception e) {
            os.close();
            socket.close();
            throw new RoundRobinRuntimeException("发送数据发生异常");
        }
        InputStream is = socket.getInputStream();
        byte[] sizeLenBytes = new byte[4];
        byte[] dataBytes = new byte[0];
        try {
            is.read(sizeLenBytes);
            int len = MessageUtils.bytes2int(sizeLenBytes);
            if(DebugConfig.DEBUG){
                System.out.println(is.available());
            }
            if (is.available() < len - 4) {
                dataBytes = new byte[is.available()];
                is.read(dataBytes);
                System.out.println(HexUtils.toDisplayString(dataBytes));
                throw new RoundRobinRuntimeException("无效报文");
            }
            dataBytes = new byte[len];
            is.read(dataBytes);
        } catch (RuntimeException e) {
            socket.close();
            throw e;
        } finally {
            os.flush();
            is.close();
            os.close();
        }
        buffer = ByteBuffer.wrap(dataBytes);
        ProtocolType protocolType = ProtocolType.valueOfCode(buffer.getInt());
        int version = buffer.getInt();
        MessageType messageType = MessageType.valueOfCode(buffer.getInt());
        if (protocolType == ProtocolType.SLICE && version == 1 && messageType == MessageType.RESPONSE) {
            protocol.convert(buffer);
            if (RspCode.valueOfCode(protocol.getCode()) == RspCode.SUCCESS) {
                //构建视图对象对象
                return new RoundRobinView(protocol.getColumns(), protocol.getTimeline(), protocol.getData());
            } else {
                throw new RoundRobinRuntimeException(protocol.getCode() + ":" + protocol.getDesc());
            }
        } else {
            System.out.println(HexUtils.toDisplayString(dataBytes));
            throw new RoundRobinRuntimeException("无效的应答");
        }
    }

    @Override
    public RoundRobinView merge(String tableName, MergeType mergeType, RoundRobinView view, int time) throws IOException {
        ifCloseThenReopenSocket();
        RoundRobinMergeProtocolV1 protocol = new RoundRobinMergeProtocolV1();
        protocol.setSessionId(sessionId);
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
        OutputStream os = socket.getOutputStream();
        try {
            os.write(data);
        } catch (Exception e) {
            os.close();
            socket.close();
            throw new RoundRobinRuntimeException("发送数据发生异常");
        }
        InputStream is = socket.getInputStream();
        byte[] sizeLenBytes = new byte[4];
        byte[] dataBytes = new byte[0];
        try {
            is.read(sizeLenBytes);
            int len = MessageUtils.bytes2int(sizeLenBytes);
            if (is.available() < len - 4) {
                System.out.println(HexUtils.toDisplayString(sizeLenBytes));
                throw new RoundRobinRuntimeException("无效报文");
            }
            dataBytes = new byte[len];
            is.read(dataBytes);
        } catch (Exception e) {
            socket.close();
        } finally {
            os.flush();
            is.close();
            os.close();
        }
        buffer = ByteBuffer.wrap(dataBytes);
        ProtocolType protocolType = ProtocolType.valueOfCode(buffer.getInt());
        int version = buffer.getInt();
        MessageType messageType = MessageType.valueOfCode(buffer.getInt());
        if (protocolType == ProtocolType.MERGE && version == 1 && messageType == MessageType.RESPONSE) {
            protocol.convert(buffer);
            if (RspCode.valueOfCode(protocol.getCode()) == RspCode.SUCCESS) {
                //构建视图对象对象
                return new RoundRobinView(protocol.getColumns(), protocol.getPos(), protocol.getData());
            } else {
                throw new RoundRobinRuntimeException(protocol.getCode() + ":" + protocol.getDesc());
            }
        } else {
            System.out.println(HexUtils.toDisplayString(dataBytes));
            throw new RoundRobinRuntimeException("无效的应答");
        }
    }


    @Override
    public TableMetadata expand(String tableName, String... columns) throws IOException {
        ifCloseThenReopenSocket();
        RoundRobinExpandProtocolV1 protocol = new RoundRobinExpandProtocolV1();
        protocol.setSessionId(sessionId);
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
        OutputStream os = socket.getOutputStream();
        try {
            os.write(data);
        } catch (Exception e) {
            os.close();
            socket.close();
            throw new RoundRobinRuntimeException("发送数据发生异常");
        }
        InputStream is = socket.getInputStream();
        byte[] sizeLenBytes = new byte[4];
        byte[] dataBytes = new byte[0];
        try {
            is.read(sizeLenBytes);
            int len = MessageUtils.bytes2int(sizeLenBytes);
            if (is.available() < len - 4) {
                System.out.println(HexUtils.toDisplayString(sizeLenBytes));
                throw new RoundRobinRuntimeException("无效报文");
            }
            dataBytes = new byte[len];
            is.read(dataBytes);
        } catch (Exception e) {
            socket.close();
        } finally {
            os.flush();
            is.close();
            os.close();
        }
        buffer = ByteBuffer.wrap(dataBytes);
        ProtocolType protocolType = ProtocolType.valueOfCode(buffer.getInt());
        int version = buffer.getInt();
        MessageType messageType = MessageType.valueOfCode(buffer.getInt());
        if (protocolType == ProtocolType.EXPAND && version == 1 && messageType == MessageType.RESPONSE) {
            protocol.convert(buffer);
            if (RspCode.valueOfCode(protocol.getCode()) == RspCode.SUCCESS) {
                //构建表元信息对象
                TableMetadata metadata = new TableMetadata(null, FormatType.BIN, protocol.getInstance(), protocol.getTableName(), protocol.getColumns(), 0, TableStatus.UNKNOWN);
                return metadata;
            } else {
                throw new RoundRobinRuntimeException(protocol.getCode() + ":" + protocol.getDesc());
            }
        } else {
            System.out.println(HexUtils.toDisplayString(dataBytes));
            throw new RoundRobinRuntimeException("无效的应答");
        }
    }

    @Override
    public TableMetadata createTable(String tableName, String... columns) throws IOException {
        ifCloseThenReopenSocket();
        RoundRobinCreateTableProtocolV1 protocol = new RoundRobinCreateTableProtocolV1();
        protocol.setSessionId(sessionId);
        protocol.setTableName(tableName);
        protocol.setColumns(columns);
        protocol.setSessionId(sessionId);
        ByteBuffer buffer = protocol.convert();
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        if (DebugConfig.DEBUG) {
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        OutputStream os = socket.getOutputStream();
        try {
            os.write(data);
        } catch (Exception e) {
            os.close();
            socket.close();
            throw new RoundRobinRuntimeException("发送数据发生异常");
        }
        InputStream is = socket.getInputStream();
        byte[] sizeLenBytes = new byte[4];
        byte[] dataBytes = new byte[0];
        try {
            is.read(sizeLenBytes);
            int len = MessageUtils.bytes2int(sizeLenBytes);
            if (is.available() < len - 4) {
                System.out.println(HexUtils.toDisplayString(sizeLenBytes));
                throw new RoundRobinRuntimeException("无效报文");
            }
            dataBytes = new byte[len];
            is.read(dataBytes);
        } catch (Exception e) {
            socket.close();
        } finally {
            os.flush();
            is.close();
            os.close();
        }
        buffer = ByteBuffer.wrap(dataBytes);
        ProtocolType protocolType = ProtocolType.valueOfCode(buffer.getInt());
        int version = buffer.getInt();
        MessageType messageType = MessageType.valueOfCode(buffer.getInt());
        if (protocolType == ProtocolType.CREATE_TABLE && version == 1 && messageType == MessageType.RESPONSE) {
            protocol.convert(buffer);
            if (RspCode.valueOfCode(protocol.getCode()) == RspCode.SUCCESS) {
                //构建表元信息对象
                TableMetadata metadata = new TableMetadata(null, FormatType.BIN, protocol.getInstance(), protocol.getTableName(), protocol.getColumns(), 0, TableStatus.UNKNOWN);
                return metadata;
            } else {
                throw new RoundRobinRuntimeException(protocol.getCode() + ":" + protocol.getDesc());
            }
        } else {
            System.out.println(HexUtils.toDisplayString(dataBytes));
            throw new RoundRobinRuntimeException("无效的应答");
        }
    }

    @Override
    public RoundRobinConnector dropTable(String... tableNames) throws IOException {
        ifCloseThenReopenSocket();
        RoundRobinDropTableProtocolV1 protocol = new RoundRobinDropTableProtocolV1();
        protocol.setTableNames(tableNames);
        protocol.setSessionId(sessionId);
        ByteBuffer buffer = protocol.convert();
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        if (DebugConfig.DEBUG) {
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        OutputStream os = socket.getOutputStream();
        try {
            os.write(data);
        } catch (Exception e) {
            os.close();
            socket.close();
            throw new RoundRobinRuntimeException("发送数据发生异常");
        }
        InputStream is = socket.getInputStream();
        byte[] sizeLenBytes = new byte[4];
        byte[] dataBytes = new byte[0];
        try {
            is.read(sizeLenBytes);
            int len = MessageUtils.bytes2int(sizeLenBytes);
            if (is.available() < len - 4) {
                System.out.println(HexUtils.toDisplayString(sizeLenBytes));
                throw new RoundRobinRuntimeException("无效报文");
            }
            dataBytes = new byte[len];
            is.read(dataBytes);
        } catch (Exception e) {
            socket.close();
        } finally {
            os.flush();
            is.close();
            os.close();
        }
        buffer = ByteBuffer.wrap(dataBytes);
        ProtocolType protocolType = ProtocolType.valueOfCode(buffer.getInt());
        int version = buffer.getInt();
        MessageType messageType = MessageType.valueOfCode(buffer.getInt());
        if (protocolType == ProtocolType.DROP_TABLE && version == 1 && messageType == MessageType.RESPONSE) {
            protocol.convert(buffer);
            if (RspCode.valueOfCode(protocol.getCode()) == RspCode.SUCCESS) {
                return this;
            } else {
                throw new RoundRobinRuntimeException(protocol.getCode() + ":" + protocol.getDesc());
            }
        } else {
            System.out.println(HexUtils.toDisplayString(dataBytes));
            throw new RoundRobinRuntimeException("无效的应答");
        }
    }

    @Override
    public RoundRobinConnector persistentTable(int persistentTime, FormatType formatType, int formatVersion, String... tableNames) throws IOException {
        ifCloseThenReopenSocket();
        RoundRobinPersistentTableProtocolV1 protocol = new RoundRobinPersistentTableProtocolV1();
        protocol.setTableNames(tableNames);
        protocol.setPersistentTime(persistentTime);
        protocol.setFormatType(formatType);
        protocol.setFormatVersion(formatVersion);
        protocol.setSessionId(sessionId);
        ByteBuffer buffer = protocol.convert();
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        if (DebugConfig.DEBUG) {
            System.out.println(data.length);
            System.out.println(HexUtils.toDisplayString(data));
        }
        OutputStream os = socket.getOutputStream();
        try {
            os.write(data);
        } catch (Exception e) {
            os.close();
            socket.close();
            throw new RoundRobinRuntimeException("发送数据发生异常");
        }
        InputStream is = socket.getInputStream();
        byte[] sizeLenBytes = new byte[4];
        byte[] dataBytes = new byte[0];
        try {
            is.read(sizeLenBytes);
            int len = MessageUtils.bytes2int(sizeLenBytes);
            if (is.available() < len - 4) {
                System.out.println(HexUtils.toDisplayString(sizeLenBytes));
                throw new RoundRobinRuntimeException("无效报文");
            }
            dataBytes = new byte[len];
            is.read(dataBytes);
        } catch (Exception e) {
            socket.close();
        } finally {
            os.flush();
            is.close();
            os.close();
        }
        buffer = ByteBuffer.wrap(dataBytes);
        ProtocolType protocolType = ProtocolType.valueOfCode(buffer.getInt());
        int version = buffer.getInt();
        MessageType messageType = MessageType.valueOfCode(buffer.getInt());
        if (protocolType == ProtocolType.PERSISTENT_TABLE && version == 1 && messageType == MessageType.RESPONSE) {
            protocol.convert(buffer);
            if (RspCode.valueOfCode(protocol.getCode()) == RspCode.SUCCESS) {
                return this;
            } else {
                throw new RoundRobinRuntimeException(protocol.getCode() + ":" + protocol.getDesc());
            }
        } else {
            System.out.println(HexUtils.toDisplayString(dataBytes));
            throw new RoundRobinRuntimeException("无效的应答");
        }
    }

    @Override
    public int execute(String sql) throws SQLException {
        return 0;
    }

    @Override
    public RoundRobinView executeQuery(String sql) throws SQLException {
        return null;
    }

}
