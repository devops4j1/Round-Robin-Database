package org.wing4j.rrd.server;

import lombok.Data;
import lombok.ToString;
import org.wing4j.rrd.RoundRobinConfig;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by wing4j on 2017/7/31.
 */
@Data
@ToString
public class RoundRobinServerConfig extends RoundRobinConfig{
    /**
     * 管理端口
     */
    int managePort;
    /**
     * 监听端口
     */
    int listenPort = 8099;
    /**
     * 数据库文件保存位置
     */
    String databaseFilePath = "D:\\";
    /**
     * 从节点
     */
    Set<String> slaves = new HashSet<>();
}
