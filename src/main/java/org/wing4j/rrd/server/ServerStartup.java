package org.wing4j.rrd.server;

import org.wing4j.rrd.RoundRobinRuntimeException;
import org.wing4j.rrd.server.aio.AioRoundRobinServer;

import java.lang.reflect.Constructor;

/**
 * Created by wing4j on 2017/8/2.
 */
public class ServerStartup {
    /**
     * 启动服务
     * @param serverClassName 服务器实现类
     * @throws Exception 异常
     */
    void startup(String serverClassName) throws Exception {
        Class serverClass = Class.forName(serverClassName);
        if(!RoundRobinServer.class.isAssignableFrom(serverClass)){
            throw new RoundRobinRuntimeException("未发现服务期实现");
        }
        Constructor constructor = serverClass.getConstructor(RoundRobinServerConfig.class);
        RoundRobinServerConfig config = new RoundRobinServerConfig();
        RoundRobinServer server = (RoundRobinServer) constructor.newInstance(config);
        server.start();
    }

    public static void main(String[] args) throws Exception {
        ServerStartup serverStartup = new ServerStartup();
        serverStartup.startup(AioRoundRobinServer.class.getName());
    }
}
