package org.wing4j.rrd.server.aio;

import org.wing4j.rrd.server.RoundRobinServer;
import org.wing4j.rrd.server.RoundRobinServerConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/8/1.
 */
public class RoundRobinListener implements Runnable {
    static Logger LOGGER = Logger.getLogger(RoundRobinListener.class.getName());
    RoundRobinServer server;

    public RoundRobinListener(RoundRobinServer server) {
        LOGGER.info("builder listener...");
        this.server = server;
    }

    @Override
    public void run() {
        RoundRobinServerConfig config = server.getConfig();
        LOGGER.info("open " + config.getListenPort() + " port.");
        ExecutorService executor = Executors.newCachedThreadPool();
        //异步通道组
        AsynchronousChannelGroup asyncChannelGroup = null;
        AsynchronousServerSocketChannel listener = null;
        try {
            asyncChannelGroup = AsynchronousChannelGroup.withThreadPool(executor);
            //监听通道
            listener = AsynchronousServerSocketChannel.open(asyncChannelGroup);
        } catch (IOException e) {
            LOGGER.warning("Round Robin Database startup happens error...");
        }

        try {
            listener.bind(new InetSocketAddress(config.getListenPort()));
        } catch (IOException e) {
            LOGGER.warning("listen port " + config.getListenPort() + " already use!");
        }
        listener.accept(listener, new RoundRobinAcceptHandler(server.getDatabase()));
        LOGGER.info("Round Robin Database startup finish...");
    }
}
