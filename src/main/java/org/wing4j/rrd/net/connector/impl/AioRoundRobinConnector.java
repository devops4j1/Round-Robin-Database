package org.wing4j.rrd.net.connector.impl;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.FormatType;
import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.core.TableMetadata;
import org.wing4j.rrd.net.connector.RoundRobinConnector;
import org.wing4j.rrd.net.protocol.RoundRobinIncreaseProtocolV1;
import org.wing4j.rrd.net.protocol.RoundRobinMergeProtocolV1;
import org.wing4j.rrd.net.protocol.RoundRobinTableMetadataProtocolV1;
import org.wing4j.rrd.utils.HexUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.sql.SQLException;
import java.util.concurrent.*;

/**
 * Created by wing4j on 2017/7/31.
 */
@Data
@ToString
public class AioRoundRobinConnector implements RoundRobinConnector {
    String address;
    int port;
    ExecutorService executor;
    AsynchronousSocketChannel socketChannel;
    AsynchronousChannelGroup asyncChannelGroup;
    RoundRobinDatabase database;

    public AioRoundRobinConnector(String address, int port, RoundRobinDatabase database) throws IOException {
        this.address = address;
        this.port = port;
        this.database = database;
        this.executor = Executors.newCachedThreadPool();
        this.asyncChannelGroup = AsynchronousChannelGroup.withThreadPool(executor);
    }


    @Override
    public String connect(String username, String password) throws IOException {
        return null;
    }

    @Override
    public void disConnect(String sessionId) throws IOException {

    }

    @Override
    public TableMetadata getTableMetadata(String tableName) throws IOException {
        RoundRobinTableMetadataProtocolV1 protocol = new RoundRobinTableMetadataProtocolV1();
        protocol.setTableName(tableName);
        ByteBuffer buffer = protocol.convert();
        buffer = syncCall(buffer);
        buffer.flip();
        protocol.convert(buffer);
        TableMetadata metadata = new TableMetadata(null, FormatType.BIN, protocol.getInstance(), protocol.getTableName(), protocol.getColumns(), protocol.getDataSize(), protocol.getStatus());
        return metadata;
    }

    @Override
    public long increase(String tableName, String column, int pos, int i) throws IOException {
        if (socketChannel == null || !socketChannel.isOpen()) {
            socketChannel = AsynchronousSocketChannel.open(asyncChannelGroup);
            socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            socketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        }
        RoundRobinIncreaseProtocolV1 protocol = new RoundRobinIncreaseProtocolV1();
        protocol.setTableName(tableName);
        protocol.setColumn(column);
        protocol.setValue(i);
        ByteBuffer buffer = protocol.convert();
        buffer = syncCall(buffer);
        buffer.flip();
        protocol.convert(buffer);
        return protocol.getNewValue();
    }

    @Override
    public long set(String tableName, String column, int pos, long i) throws IOException {
        return 0;
    }

    @Override
    public long get(String tableName, String column, int pos) throws IOException {
        return 0;
    }

    @Override
    public RoundRobinView slice(String tableName, int pos, int size, String... columns) throws IOException {
        return null;
    }

    @Override
    public RoundRobinView merge(String tableName, MergeType mergeType, RoundRobinView view, int pos) throws IOException {
        AsynchronousSocketChannel socketChannel = null;
        if (socketChannel == null || !socketChannel.isOpen()) {
            socketChannel = AsynchronousSocketChannel.open(asyncChannelGroup);
            socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            socketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        }
        RoundRobinMergeProtocolV1 protocol = new RoundRobinMergeProtocolV1();
        protocol.setData(view.getData());
        protocol.setSize(view.getData().length);
        protocol.setColumns(view.getMetadata().getColumns());
        protocol.setPos(pos);
        protocol.setMergeType(mergeType);
        protocol.setTableName(tableName);
        final ByteBuffer buffer = protocol.convert();
        socketChannel.connect(new InetSocketAddress(address, port), socketChannel, new CompletionHandler<Void, AsynchronousSocketChannel>() {

            @Override
            public void completed(Void result, AsynchronousSocketChannel connector) {
                buffer.flip();
                Future writeResult = connector.write(buffer);
                try {
                    writeResult.get(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
                System.out.println("------------------------------------------------------------------------");
                buffer.clear();
                Future readResult = connector.read(buffer);
                try {
                    readResult.get(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
                buffer.flip();
                System.out.println(HexUtils.toDisplayString(buffer.array()));
            }

            @Override
            public void failed(Throwable exc, AsynchronousSocketChannel attachment) {

            }
        });
        protocol.convert(buffer);
        System.out.println(protocol);
        return null;
    }


    @Override
    public TableMetadata expand(String tableName, String... columns) throws IOException {
        return null;
    }

    @Override
    public TableMetadata createTable(String tableName, String... columns) throws IOException {
        return null;
    }

    @Override
    public RoundRobinConnector dropTable(String... tableNames) throws IOException {
        return null;
    }

    @Override
    public RoundRobinConnector persistentTable(String[] tableNames, int persistentTime) throws IOException {
        return null;
    }

    @Override
    public int execute(String sql) throws SQLException {
        return 0;
    }

    @Override
    public RoundRobinView executeQuery(String sql) throws SQLException {
        return null;
    }

    /**
     * 同步发送信息
     * @param buffer
     * @return
     */
    ByteBuffer syncCall(ByteBuffer buffer){
        Future connectFuture = socketChannel.connect(new InetSocketAddress(address, port));
        try {
            connectFuture.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        while (buffer.hasRemaining()){
            Future writeFuture = socketChannel.write(buffer);
            try {
                writeFuture.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        buffer.clear();
        Future readFuture = socketChannel.read(buffer);
        try {
            readFuture.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return buffer;
    }
}
