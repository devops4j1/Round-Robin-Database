package org.wing4j.rrd.net.acceptor;

/**
 * Created by woate on 2017/8/8.
 */
public interface RoundRobinAcceptor {

    void start();

    String getName();

    int getPort();
}
