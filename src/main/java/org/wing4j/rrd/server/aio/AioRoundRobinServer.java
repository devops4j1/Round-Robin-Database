package org.wing4j.rrd.server.aio;

import lombok.Data;
import org.wing4j.rrd.FormatType;
import org.wing4j.rrd.RoundRobinConnection;
import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.core.DefaultRoundRobinDatabase;
import org.wing4j.rrd.core.Table;
import org.wing4j.rrd.debug.DebugConfig;
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
public class AioRoundRobinServer implements RoundRobinServer {
    static Logger LOGGER = Logger.getLogger(AioRoundRobinServer.class.getName());
    RoundRobinServerConfig serverConfig;
    RoundRobinListener listener;
    Thread listenThread;
    RoundRobinDatabase database;
    int status = STOP;

    public AioRoundRobinServer(RoundRobinServerConfig serverConfig) {
        this.serverConfig = serverConfig;
        try {
            LOGGER.info("ready to init [default] database.");
            this.database = DefaultRoundRobinDatabase.init(serverConfig);
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
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOGGER.info("begin to persistent table !");
                if (DebugConfig.DEBUG) {
                    database.persistent(FormatType.CSV, 1, 0);
                } else {
                    database.persistent(FormatType.BIN, 1, 0);
                }
                LOGGER.info("persistent table finish!");
            }
        });
        while (status == RUNNING) {
            Thread.sleep(60 * 1000);
        }
    }

    @Override
    public void stop() {
        listenThread.interrupt();
    }
}
