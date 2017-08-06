package org.wing4j.rrd;

import org.wing4j.rrd.core.Table;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by wing4j on 2017/7/28.
 */
public interface RoundRobinDatabase {
    int DAY_SECOND = 24 * 60 * 60;

    /**
     *
     * @return
     */
    Map<String, RoundRobinConnection> getConnections();
    /**
     * 根据会话ID号获取连接信息
     * @param sessionId 会话ID号
     * @return
     */
    RoundRobinConnection getConnection(String sessionId);
    /**
     * 打开远程数据库
     *
     * @param address 服务器地址
     * @param port    端口
     * @return 连接对象
     */
    RoundRobinConnection open(String address, int port, String username, String password) throws IOException;

    /**
     * 打开数据库
     *
     * @return 连接对象
     */
    RoundRobinConnection open() throws IOException;

    Table getTable(String tableName);
    /**
     * 创建表
     *
     * @param tableName 表名
     * @param columns   字段数组
     * @return
     */
    RoundRobinDatabase createTable(String tableName, String... columns) throws IOException;

    /**
     * 从文件打开表
     * @param file 文件
     * @return 数据库对象
     * @throws IOException
     */
    RoundRobinDatabase openTable(File file) throws IOException;

    /**
     * 删除表
     *
     * @param tableNames
     * @return
     */
    RoundRobinDatabase dropTable(String... tableNames) throws IOException;

    /**
     * 打开配置对象
     *
     * @return
     */
    RoundRobinConfig getConfig();

    /**
     * 创建计划持久化任务
     * @param tableName 表
     * @param persistentTime 持久化时间
     * @return
     */
    RoundRobinDatabase persistent(String tableName, int persistentTime);
    /**
     * 关闭数据库
     */
    void close() throws IOException;

    boolean existTable(String tableName, boolean throwException);

    List<Table> listTable(String ... tableNames);
    /**
     * 关闭指定的连接
     *
     * @param connection
     * @throws IOException
     */
    void close(RoundRobinConnection connection) throws IOException;
}
