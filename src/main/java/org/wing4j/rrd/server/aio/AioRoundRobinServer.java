package org.wing4j.rrd.server.aio;

import lombok.Data;
import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.core.DefaultRoundRobinDatabase;
import org.wing4j.rrd.server.RoundRobinServer;
import org.wing4j.rrd.server.RoundRobinServerConfig;

import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/7/31.
 */
@Data
public class AioRoundRobinServer implements RoundRobinServer{
    static Logger LOGGER = Logger.getLogger(AioRoundRobinServer.class.getName());
    RoundRobinServerConfig config;
    RoundRobinListener listener;
    Thread listenThread;
    RoundRobinDatabase database;

    public AioRoundRobinServer(RoundRobinServerConfig config) {
        this.config = config;
        this.database = DefaultRoundRobinDatabase.init(config);
    }


    @Override
    public void start() throws InterruptedException {
        LOGGER.info("start listener.");
        this.listener = new RoundRobinListener(this);
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
