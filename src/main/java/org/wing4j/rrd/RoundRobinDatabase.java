package org.wing4j.rrd;

import java.io.IOException;

/**
 * Created by wing4j on 2017/7/28.
 */
public interface RoundRobinDatabase {
    /**
     * 创建数据库文件
     * @param fileName 持久化
     * @return
     * @throws IOException
     */
    RoundRobinConnection create(String fileName) throws IOException;
    /**
     * 打开远程数据库
     * @param address 服务器地址
     * @param port 端口
     * @return 连接对象
     */
    RoundRobinConnection open(String address, int port) throws IOException;
    /**
     * 打开数据库
     * @param fileName 文件名
     * @param names 表头
     * @return 连接对象
     */
    RoundRobinConnection open(String fileName, String... names) throws IOException ;
    /**
     * 打开数据库
     * @param fileName 文件名
     * @return 连接对象
     */
    RoundRobinConnection open(String fileName) throws IOException;

    /**
     * 打开配置对象
     * @return
     */
    RoundRobinConfig getConfig();
    /**
     * 关闭数据库
     */
    void close() throws IOException;

    /**
     * 关闭指定的连接
     * @param connection
     * @throws IOException
     */
    void close(RoundRobinConnection connection) throws IOException;
}
