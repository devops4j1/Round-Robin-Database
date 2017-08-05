package org.wing4j.rrd.server.aio;

import lombok.Data;
import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.core.DefaultRoundRobinDatabase;
import org.wing4j.rrd.server.RoundRobinServer;
import org.wing4j.rrd.server.RoundRobinServerConfig;

import java.io.IOException;
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
        try {
            LOGGER.info("ready to init database.");
            this.database = DefaultRoundRobinDatabase.init(config);
            LOGGER.info("init database.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void start() throws InterruptedException, IOException {
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
