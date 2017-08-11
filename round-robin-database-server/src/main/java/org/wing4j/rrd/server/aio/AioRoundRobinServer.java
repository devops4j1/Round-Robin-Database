package org.wing4j.rrd.server.aio;

import lombok.Data;
import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.core.DefaultRoundRobinDatabase;
import org.wing4j.rrd.core.RemoteRoundRobinDatabase;
import org.wing4j.rrd.net.listener.aio.AioRoundRobinListener;
import org.wing4j.rrd.server.RoundRobinServer;
import org.wing4j.rrd.server.RoundRobinServerConfig;
import org.wing4j.rrd.server.ServerInterceptor;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Created by wing4j on 2017/7/31.
 */
@Data
public class AioRoundRobinServer implements RoundRobinServer {
    static Logger LOGGER = Logger.getLogger(AioRoundRobinServer.class.getName());
    RoundRobinServerConfig serverConfig;
    AioRoundRobinListener listener;
    Thread listenThread;
    RoundRobinDatabase database;
    int status = STOP;

    public AioRoundRobinServer(RoundRobinServerConfig serverConfig) {
        this.serverConfig = serverConfig;
        try {
            LOGGER.info("ready to init [default] database.");
            this.database = RemoteRoundRobinDatabase.init(serverConfig);
            LOGGER.info("init database.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void registerInterceptor(ServerInterceptor interceptor) {

    }

    @Override
    public void start() throws InterruptedException, IOException {
        LOGGER.info("start listener.");
        this.listener = new AioRoundRobinListener(this);
        this.listenThread = new Thread(listener, "Round-Robin-Database-listener");
        this.listenThread.start();
        LOGGER.info("startup finish.");
        status = RUNNING;
        while (status == RUNNING) {
            Thread.sleep(60 * 1000);
        }
    }

    @Override
    public void stop() {
        status = STOP;
        LOGGER.warning("Round Robin Database is shutdown now!");
        System.exit(15);
    }
}
