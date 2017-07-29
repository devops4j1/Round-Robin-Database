package org.wing4j.rrd;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by wing4j on 2017/7/28.
 */
public interface RoundRobinDatabase {
    /**
     * 初始化数据库
     *
     * @param names 表头
     * @return 初始化成功返回真
     */
    RoundRobinConnection open(int time, TimeUnit timeUnit, String... names);
    /**
     * 打开数据库
     *
     * @param fileName 文件名
     */
    RoundRobinConnection open(String fileName) throws IOException;

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
