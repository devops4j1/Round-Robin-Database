package org.wing4j.rrd.net.connector;

import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.core.TableMetadata;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by wing4j on 2017/7/31.
 * 连接器
 */
public interface RoundRobinConnector {
    /**
     * 通信地址
     *
     * @return
     */
    String getAddress();

    /**
     * 端口号
     *
     * @return
     */
    int getPort();

    /**
     * 建立连接
     *
     * @return
     */
    String connect(String username, String password) throws IOException;

    /**
     * 断开连接
     *
     * @param sessionId 会话
     */
    void disConnect(String sessionId) throws IOException;

    /**
     * 获取表对象
     *
     * @param tableName 表名
     * @return 表对象
     * @throws IOException
     */
    TableMetadata getTableMetadata(String tableName) throws IOException;

    /**
     * 自增
     *
     * @param tableName 表名
     * @param column    字段名
     * @param i         自增量
     * @return 自增后的值
     * @throws IOException
     */
    long increase(String tableName, String column, int pos, int i) throws IOException;

    /**
     * 读取数据
     *
     * @param tableName 表名
     * @param pos       读取结束的偏移位置
     * @param size      读取数据长度
     * @param columns   字段名
     * @return 视图切片
     * @throws IOException
     */
    RoundRobinView slice(String tableName, int pos, int size, String... columns) throws IOException;

    /**
     * 写入数据
     *
     * @param tableName 表名
     * @param mergeType 合并类型
     * @param view      视图切片
     * @param pos       写入结束的偏移位置
     * @return
     * @throws IOException
     */
    RoundRobinView merge(String tableName, MergeType mergeType, RoundRobinView view, int pos) throws IOException;

    /**
     * 字段增加
     *
     * @param tableName 表名
     * @param columns   增加的字段数组
     * @return
     * @throws IOException
     */
    TableMetadata expand(String tableName, String... columns) throws IOException;

    /**
     * 创建表结构
     *
     * @param tableName 表名
     * @param columns   字段数组
     * @return
     */
    TableMetadata createTable(String tableName, String... columns) throws IOException;

    /**
     * 删除表结构
     *
     * @param tableNames 表名数组
     * @return
     */
    RoundRobinConnector dropTable(String... tableNames) throws IOException;

    /**
     * 持久化表数据
     * @param tableNames
     * @return
     * @throws SQLException
     */
    RoundRobinConnector persistentTable(String[] tableNames, int persistentTime) throws IOException;

    /**
     * 执行SQL语句
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    int execute(String sql) throws SQLException;

    /**
     * 执行SQL语句查询
     *
     * @param sql SQL语句
     * @return 视图
     * @throws SQLException
     */
    RoundRobinView executeQuery(String sql) throws SQLException;

}
