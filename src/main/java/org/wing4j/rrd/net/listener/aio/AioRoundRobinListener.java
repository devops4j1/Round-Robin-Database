package org.wing4j.rrd.net.listener.aio;

import org.wing4j.rrd.FormatType;
import org.wing4j.rrd.debug.DebugConfig;
import org.wing4j.rrd.net.listener.RoundRobinListener;
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
public class AioRoundRobinListener extends Thread implements RoundRobinListener {
    static Logger LOGGER = Logger.getLogger(AioRoundRobinListener.class.getName());
    RoundRobinServer server;

    public AioRoundRobinListener(RoundRobinServer server) {
        LOGGER.info("builder listener...");
        this.server = server;
    }

    @Override
    public void run() {
        RoundRobinServerConfig serverConfig = server.getServerConfig();
        LOGGER.info("open " + serverConfig.getListenPort() + " port.");
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
            server.stop();
            return;
        }

        try {
            listener.bind(new InetSocketAddress(serverConfig.getListenPort()));
            listener.accept(listener, new AioRoundRobinAcceptHandler(serverConfig, server.getDatabase()));
            LOGGER.info("Round Robin Database startup finish...");
        } catch (IOException e) {
            LOGGER.warning("listen port " + serverConfig.getListenPort() + " already use!");
            server.stop();
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOGGER.info("begin to persistent table !");
                if (DebugConfig.DEBUG) {
                    server.getDatabase().persistent(FormatType.CSV, 1, 0);
                } else {
                    server.getDatabase().persistent(FormatType.BIN, 1, 0);
                }
                LOGGER.info("persistent table finish!");
            }
        });

    }

    @Override
    public int getPort() {
        return 0;
    }
}
