package org.wing4j.rrd.server.nio;

import lombok.Getter;
import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.core.DefaultRoundRobinDatabase;
import org.wing4j.rrd.net.listener.RoundRobinListener;
import org.wing4j.rrd.net.listener.nio.NioRoundRobinListener;
import org.wing4j.rrd.server.RoundRobinServer;
import org.wing4j.rrd.server.RoundRobinServerConfig;
import org.wing4j.rrd.server.ServerInterceptor;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Created by woate on 2017/8/8.
 */
public class NioRoundRobinServer implements RoundRobinServer{
    static Logger LOGGER = Logger.getLogger(NioRoundRobinServer.class.getName());
    @Getter
    RoundRobinServerConfig serverConfig;
    @Getter
    RoundRobinDatabase database;
    RoundRobinListener listener;
    int status = STOP;

    public NioRoundRobinServer(RoundRobinServerConfig serverConfig) throws IOException {
        this.serverConfig = serverConfig;
        this.listener = new NioRoundRobinListener(this);
        try {
            LOGGER.info("ready to init [default] database.");
            this.database = DefaultRoundRobinDatabase.init(serverConfig);
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
        this.listener.start();
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
