package org.wing4j.rrd.server.impl;

import org.wing4j.rrd.server.RoundRobinServerConfig;

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
    RoundRobinServerConfig config;


    public RoundRobinListener(RoundRobinServerConfig config) {
        LOGGER.info("builder listener...");
        this.config = config;
    }

    @Override
    public void run() {
        try {
            LOGGER.info("open " + config.getListenPort() + " port.");
            ExecutorService executor = Executors.newCachedThreadPool();
            //异步通道组
            AsynchronousChannelGroup asyncChannelGroup = AsynchronousChannelGroup.withThreadPool(executor);
            //监听通道
            AsynchronousServerSocketChannel listener = AsynchronousServerSocketChannel.open(asyncChannelGroup).bind(new InetSocketAddress(config.getListenPort()));
            listener.accept(listener, new RoundRobinAcceptHandler());
            LOGGER.info("Round Robin Database startup finish...");
        } catch (Exception e) {
            LOGGER.warning("Round Robin Database startup happens error...");
        } finally {

        }
    }
}
