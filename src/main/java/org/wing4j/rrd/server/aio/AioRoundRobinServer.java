package org.wing4j.rrd.server.aio;

import lombok.Data;
import org.wing4j.rrd.RoundRobinConnection;
import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.core.DefaultRoundRobinDatabase;
import org.wing4j.rrd.server.RoundRobinServer;
import org.wing4j.rrd.server.RoundRobinServerConfig;
import org.wing4j.rrd.utils.MessageFormatter;

import java.io.IOException;
import java.util.Map;
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
    int status = STOP;

    public AioRoundRobinServer(RoundRobinServerConfig config) {
        this.config = config;
        try {
            LOGGER.info("ready to init [default] database.");
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
        status = RUNNING;
        while (status == RUNNING){
            Map<String, RoundRobinConnection> connections = database.getConnections();
            for (String session : connections.keySet()){
                LOGGER.info(MessageFormatter.format("SessionId:{}, Connection:{}", session, connections.get(session)));
            }
            Thread.sleep(60 * 1000);
        }
    }

    @Override
    public void stop() {
        listenThread.interrupt();
    }
}
