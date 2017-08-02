package org.wing4j.rrd.server.impl;

import lombok.Data;
import org.wing4j.rrd.RoundRobinConfig;
import org.wing4j.rrd.server.RoundRobinServer;
import org.wing4j.rrd.server.RoundRobinServerConfig;

import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/7/31.
 */
@Data
public class DefaultRoundRobinServer implements RoundRobinServer{
    static Logger LOGGER = Logger.getLogger(DefaultRoundRobinServer.class.getName());
    RoundRobinServerConfig config;
    RoundRobinListener listener;
    Thread listenThread;

    private DefaultRoundRobinServer(RoundRobinServerConfig config) {
        this.config = config;
    }

    public static RoundRobinServer init(RoundRobinServerConfig config) {
        LOGGER.info("Round Robin Database init.");
        return new DefaultRoundRobinServer(config);
    }

    public static RoundRobinServer init(String configName) {
        LOGGER.info("Round Robin Database init.");
        RoundRobinServerConfig config = new RoundRobinServerConfig();
        return init(config);
    }

    @Override
    public void start() throws InterruptedException {
        LOGGER.info("start listener.");
        this.listener = new RoundRobinListener(this.config);
        this.listenThread = new Thread(listener, "Round-Robin-Database-listener");
        this.listenThread.start();
        while (true){
            Thread.sleep(1000);
        }
    }

    @Override
    public void stop() {
        listenThread.interrupt();
    }
}
