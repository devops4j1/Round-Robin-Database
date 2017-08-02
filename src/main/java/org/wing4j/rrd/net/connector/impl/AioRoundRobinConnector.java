package org.wing4j.rrd.net.connector.impl;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.RoundRobinFormat;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.net.connector.RoundRobinConnector;
import org.wing4j.rrd.net.format.RoundRobinFormatNetworkV1;
import org.wing4j.rrd.utils.HexUtils;

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
        RoundRobinFormat format;

        public ConnectHandler(RoundRobinFormat format) {
            this.format = format;
        }


        @Override
        public void completed(Void result, AsynchronousSocketChannel connector) {
            ByteBuffer byteBuffer = format.write();
            byteBuffer.flip();
            Future writeResult = connector.write(byteBuffer);
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
    public RoundRobinView read(int time, int size, String... names) {
        return null;
    }

    @Override
    public RoundRobinConnector write(RoundRobinView view, int time, MergeType mergeType) throws IOException {
        AsynchronousSocketChannel socketChannel = null;
        if (socketChannel == null || !socketChannel.isOpen()) {
            socketChannel = AsynchronousSocketChannel.open(asyncChannelGroup);
            socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            socketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        }
        RoundRobinFormat format = new RoundRobinFormatNetworkV1(mergeType, time, view);
        socketChannel.connect(new InetSocketAddress(address, port), socketChannel, new ConnectHandler(format));
        return this;
    }
}
