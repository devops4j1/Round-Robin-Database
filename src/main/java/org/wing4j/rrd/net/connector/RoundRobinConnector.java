package org.wing4j.rrd.net.connector;

import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.RoundRobinView;

import java.io.IOException;

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
    RoundRobinView read(int time, int size, String tableName, String... names);

    /**
     * 写入数据
     * @param view
     */
    RoundRobinConnector write(String tableName, int pos,RoundRobinView view, MergeType mergeType) throws IOException;
}
