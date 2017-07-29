package org.wing4j.rrd;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by wing4j on 2017/7/29.
 */
public interface RoundRobinConnection {
    /**
     * 获取数据库
     * @return
     */
    RoundRobinDatabase getDatabase();
    /**
     * 获取表头
     * @return
     */
    String[] getHeader();

    /**
     * 读取所有的数据
     * @param name
     * @return
     */
    long[][] read(String... name);
    /**
     * 是否包含表头
     * @param name 表头名字
     * @return
     */
    boolean contain(String name);
    /**
     * 对应表头数据自增1
     * @param sec
     * @param name
     */
    RoundRobinConnection increase(int sec,String name);

    /**
     * 对应表头数据自增1
     * @param name
     * @return
     */
    RoundRobinConnection increase(String name);

    /**
     * 对应表头数据自增i
     * @param name
     * @param i 增量
     */
    RoundRobinConnection increase(String name, int i);

    /**
     * 对应表头数据自增i
     * @param sec
     * @param name
     * @param i
     * @return
     */
    RoundRobinConnection increase(int sec,String name, int i);

    /**
     * 设置持久化文件名
     * @param fileName
     * @return
     */
    RoundRobinConnection persistent(String fileName) throws IOException;

    /**
     * 关闭连接
     */
    void close() throws IOException;

    /**
     * 转换成流输出
     * @return
     */
    InputStream toStream();
}
