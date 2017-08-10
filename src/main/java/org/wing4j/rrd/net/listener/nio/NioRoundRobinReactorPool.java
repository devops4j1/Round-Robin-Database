package org.wing4j.rrd.net.listener.nio;

import org.wing4j.rrd.server.RoundRobinServer;

import java.io.IOException;

/**
 * Created by wing4j on 2017/8/10.
 * Nio反射器池
 */
public class NioRoundRobinReactorPool {
    private final NioRoundRobinReactor[] reactors;
    private volatile int nextReactor;

    public NioRoundRobinReactorPool(RoundRobinServer server, String name, int poolSize) throws IOException {
        reactors = new NioRoundRobinReactor[poolSize];
        for (int i = 0; i < poolSize; i++) {
            NioRoundRobinReactor reactor = new NioRoundRobinReactor(name + "-" + i, server);
            reactors[i] = reactor;
            reactor.startup();
        }
    }

    public NioRoundRobinReactorPool startup(){
        for (NioRoundRobinReactor reactor : reactors) {
            reactor.startup();
        }
        return this;
    }

    /**
     * 顺序获取反射器
     * @return
     */
    public NioRoundRobinReactor getNextReactor() {
        int i = ++nextReactor;
        if (i >= reactors.length) {
            i = nextReactor = 0;
        }
        return reactors[i];
    }
}
