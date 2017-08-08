package org.wing4j.rrd.server.nio;

import lombok.Getter;
import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.server.RoundRobinServer;
import org.wing4j.rrd.server.RoundRobinServerConfig;
import org.wing4j.rrd.server.aio.RoundRobinListener;

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
    int status = STOP;

    @Override
    public void start() throws InterruptedException, IOException {

    }

    @Override
    public void stop() {

    }
}
