package org.wing4j.rrd.core.engine;

import org.junit.Test;
import org.wing4j.rrd.core.Table;
import org.wing4j.rrd.core.TableMetadata;
import org.wing4j.rrd.net.connector.RoundRobinConnector;
import org.wing4j.rrd.net.connector.impl.AioRoundRobinConnector;
import org.wing4j.rrd.net.connector.impl.BioRoundRobinConnector;

import static org.junit.Assert.*;

/**
 * Created by 面试1 on 2017/8/5.
 */
public class RemoteTableTest {

    @Test
    public void testGetMetadata() throws Exception {
        RoundRobinConnector connector = new BioRoundRobinConnector("127.0.0.0", 8099);
        Table table = new RemoteTable("mo9", connector);
        long i = table.increase("success");
        System.out.println(i);
    }
}