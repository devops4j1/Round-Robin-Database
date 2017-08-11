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
     * 重置连接计数器阈值
     */
    int rebuildCountThreshold = 512;
    /**
     *
     */
    long minSelectTimeInNanoSeconds = 500000L;
    /**
     * 最大反射器数量
     */
    int maxReactorSize = 10;
    /**
     * 从节点
     */
    Set<String> slaves = new HashSet<>();
}
