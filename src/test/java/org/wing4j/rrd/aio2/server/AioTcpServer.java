package org.wing4j.rrd.aio2.server;

import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AioTcpServer implements Runnable {
    private AsynchronousChannelGroup asyncChannelGroup;
    private AsynchronousServerSocketChannel listener;

    public AioTcpServer(int port) throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        asyncChannelGroup = AsynchronousChannelGroup.withThreadPool(executor);
        listener = AsynchronousServerSocketChannel.open(asyncChannelGroup).bind(new InetSocketAddress(port));
    }

    public void run() {
        try {
            listener.accept(listener, new AioAcceptHandler());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("finished server");
        }
    }

    public static void main(String... args) throws Exception {
        AioTcpServer server = new AioTcpServer(9008);
        new Thread(server).start();
        while (true) {
            System.out.println(Thread.activeCount());
            Thread.sleep(1000);
        }
    }
}