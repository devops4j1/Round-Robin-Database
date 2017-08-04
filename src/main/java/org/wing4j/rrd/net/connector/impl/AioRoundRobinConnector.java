package org.wing4j.rrd.net.connector.impl;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.core.TableMetadata;
import org.wing4j.rrd.net.connector.RoundRobinConnector;
import org.wing4j.rrd.net.protocol.RoundRobinMergeProtocolV1;
import org.wing4j.rrd.net.protocol.RoundRobinTableMetadataProtocolV1;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
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

    static class ConnectHandler implements CompletionHandler<Void, AsynchronousSocketChannel>{
        ByteBuffer buffer;
        RoundRobinConnector connector;

        public ConnectHandler(ByteBuffer buffer, RoundRobinConnector connector) {
            this.buffer = buffer;
            this.connector = connector;
        }

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
            ByteBuffer readBuffer = ByteBuffer.allocate(1024);
            Future readResult = connector.read(readBuffer);
            try {
                readResult.get(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
            readBuffer.flip();
            System.out.println(new String(readBuffer.array()).trim());
        }

        @Override
        public void failed(Throwable exc, AsynchronousSocketChannel connector) {
            exc.printStackTrace();
        }
    }
    public AioRoundRobinConnector(String address, int port) throws IOException {
        this.address = address;
        this.port = port;
        this.executor = Executors.newCachedThreadPool();
        this.asyncChannelGroup = AsynchronousChannelGroup.withThreadPool(executor);
    }

    @Override
    public TableMetadata getTableMetadata(String tableName) throws IOException {
        AsynchronousSocketChannel socketChannel = null;
        if (socketChannel == null || !socketChannel.isOpen()) {
            socketChannel = AsynchronousSocketChannel.open(asyncChannelGroup);
            socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            socketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        }
        RoundRobinTableMetadataProtocolV1 protocol = new RoundRobinTableMetadataProtocolV1();
        protocol.setTableName(tableName);
        ByteBuffer buffer = protocol.convert();
        socketChannel.connect(new InetSocketAddress(address, port), socketChannel, new ConnectHandler(buffer, this));
        return null;
    }

    @Override
    public long increase(String tableName, String column, int i) throws IOException {
        return 0;
    }
    @Override
    public RoundRobinView slice(int pos, int size, String tableName, String... columns) throws IOException {
        return null;
    }

    @Override
    public RoundRobinConnector merge(String tableName, int time, RoundRobinView view, MergeType mergeType) throws IOException {
        AsynchronousSocketChannel socketChannel = null;
        if (socketChannel == null || !socketChannel.isOpen()) {
            socketChannel = AsynchronousSocketChannel.open(asyncChannelGroup);
            socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            socketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        }
        RoundRobinMergeProtocolV1 protocol = new RoundRobinMergeProtocolV1();
        protocol.setData(view.getData());
        protocol.setColumns(view.getMetadata().getColumns());
        protocol.setCurrent(time);
        protocol.setMergeType(mergeType);
        protocol.setTableName(tableName);
        ByteBuffer buffer = protocol.convert();
        socketChannel.connect(new InetSocketAddress(address, port), socketChannel, new ConnectHandler(buffer, this));
        return this;
    }

    @Override
    public RoundRobinConnector merge(String tableName, RoundRobinView view, MergeType mergeType) throws IOException {
        AsynchronousSocketChannel socketChannel = null;
        if (socketChannel == null || !socketChannel.isOpen()) {
            socketChannel = AsynchronousSocketChannel.open(asyncChannelGroup);
            socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            socketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        }
        RoundRobinMergeProtocolV1 protocol = new RoundRobinMergeProtocolV1();
        protocol.setData(view.getData());
        protocol.setColumns(view.getMetadata().getColumns());
        protocol.setCurrent(view.getTime());
        protocol.setMergeType(mergeType);
        protocol.setTableName(tableName);
        ByteBuffer buffer = protocol.convert();
        socketChannel.connect(new InetSocketAddress(address, port), socketChannel, new ConnectHandler(buffer, this));
        return this;
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
