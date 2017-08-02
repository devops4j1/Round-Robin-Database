package org.wing4j.rrd.server;

import lombok.Data;
import lombok.ToString;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by wing4j on 2017/7/31.
 */
@Data
@ToString
public class RoundRobinServerConfig {
    /**
     * 管理端口
     */
    int managePort;
    /**
     * 监听端口
     */
    int listenPort = 8099;
    /**
     * 从节点
     */
    Set<String> slaves = new HashSet<>();
}
