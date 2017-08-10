package org.wing4j.rrd.net.listener;

/**
 * Created by wing4j on 2017/8/8.
 * 监听器
 */
public interface RoundRobinListener {
    /**
     * 启动服务
     */
    void start();

    /**
     * 监听器名称
     * @return
     */
    String getName();

    /**
     * 监听端口
     * @return
     */
    int getPort();
}
