package org.wing4j.rrd.server;

import org.wing4j.rrd.server.impl.DefaultRoundRobinServer;

/**
 * Created by wing4j on 2017/8/2.
 */
public class ServerStartup {
    public static void main(String[] args) throws InterruptedException {
//        String configName = args[0];
        RoundRobinServerConfig config = new RoundRobinServerConfig();
        RoundRobinServer server = DefaultRoundRobinServer.init(config);
        server.start();
    }
}
