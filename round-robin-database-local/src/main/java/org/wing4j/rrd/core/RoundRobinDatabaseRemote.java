package org.wing4j.rrd.core;

import org.wing4j.rrd.RoundRobinConnection;
import org.wing4j.rrd.RoundRobinDatabase;
import org.wing4j.rrd.core.Table;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by wing4j on 2017/7/28.
 */
public interface RoundRobinDatabaseRemote extends RoundRobinDatabase{
    /**
     * 打开远程数据库
     *
     * @param address 服务器地址
     * @param port    端口
     * @return 连接对象
     */
    RoundRobinConnection open(String address, int port, String username, String password) throws IOException;
}
