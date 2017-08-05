package org.wing4j.rrd.server;

import org.wing4j.rrd.RoundRobinDatabase;

import java.io.IOException;

/**
 * Created by wing4j on 2017/7/31.
 * 服务器接口
 */
public interface RoundRobinServer {
    int RUNNING = 1;
    int STOP = 0;
    /**
     * 获取配置对象
     * @return 配置对象
     */
    RoundRobinServerConfig getConfig();

    RoundRobinDatabase getDatabase();

    /**
     * 启动数据库，开始监听
     * @return 服务器对象
     */
    void start() throws InterruptedException, IOException;

    /**
     * 停止数据库，停止监听
     * @return 服务器对象
     */
    void stop();
}
