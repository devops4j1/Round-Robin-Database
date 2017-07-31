package org.wing4j.rrd.server;

/**
 * Created by wing4j on 2017/7/31.
 * 服务器接口
 */
public interface RoundRobinServer {
    /**
     * 初始化服务器
     * @param config 配置对象
     * @return 服务器对象
     */
    RoundRobinServer init(RoundRobinServerConfig config);

    /**
     * 初始化服务器
     * @param configName 配置文件
     * @return 服务器对象
     */
    RoundRobinServer init(String configName);

    /**
     * 获取配置对象
     * @return 配置对象
     */
    RoundRobinServerConfig getConfig();

    /**
     * 重新配置数据库
     * @param config 配置对象
     * @return 服务器对象
     */
    RoundRobinServer setConfig(RoundRobinServerConfig config);

    /**
     * 启动数据库，开始监听
     * @return 服务器对象
     */
    RoundRobinServer start();

    /**
     * 停止数据库，停止监听
     * @return 服务器对象
     */
    RoundRobinServer stop();

    /**
     * 关闭数据库
     * @return 服务器对象
     */
    RoundRobinServer close();

}
