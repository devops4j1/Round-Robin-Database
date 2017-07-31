package org.wing4j.rrd.net.connector;

/**
 * Created by wing4j on 2017/7/31.
 * 连接器
 */
public interface RoundRobinConnector {
    /**
     * 通信地址
     * @return
     */
    String getAddress();

    /**
     * 端口号
     * @return
     */
    int getPort();
    /**
     * 读取数据
     * @param time
     * @param size
     * @param names
     * @return
     */
    long[][] read(int time, int size, String... names);

    /**
     * 写入数据
     * @param time
     * @param data
     * @param names
     */
    RoundRobinConnector write(int time, long[][] data, String... names);
    RoundRobinConnector start();
    RoundRobinConnector close();
}
