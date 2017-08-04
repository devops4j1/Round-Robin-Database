package org.wing4j.rrd;

import org.wing4j.rrd.core.Table;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by wing4j on 2017/7/28.
 */
public interface RoundRobinDatabase {
    int DAY_SECOND = 24 * 60 * 60;

    /**
     * 打开远程数据库
     *
     * @param address 服务器地址
     * @param port    端口
     * @return 连接对象
     */
    RoundRobinConnection open(String address, int port) throws IOException;

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
    RoundRobinDatabase createTable(File file) throws IOException;

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
