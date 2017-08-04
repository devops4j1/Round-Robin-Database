package org.wing4j.rrd.net.connector.impl;

import org.junit.Test;
import org.wing4j.rrd.*;
import org.wing4j.rrd.core.DefaultRoundRobinDatabase;
import org.wing4j.rrd.core.format.csv.v1.RoundRobinFormatCsvV1;
import org.wing4j.rrd.net.connector.RoundRobinConnector;

import static org.junit.Assert.*;

/**
 * Created by wing4j on 2017/8/2.
 */
public class AioRoundRobinConnectorTest {

    @Test
    public void testWrite() throws Exception {
        RoundRobinDatabase database = DefaultRoundRobinDatabase.init(new RoundRobinConfig());
        RoundRobinConnection connection = database.open();
        connection.createTable("mo9", "request", "response");
        RoundRobinView view = connection.slice("mo9", 60 * 60, connection.getColumns("mo9"));
        RoundRobinFormat format = new RoundRobinFormatCsvV1("", view);
        format.write("D:/22.csv");
        for (int i = 0; i < 1; i++) {
            RoundRobinConnector connector = new AioRoundRobinConnector("127.0.0.1", 8099);
            connector.write("mo9", 0, view, MergeType.ADD);
        }
        Thread.sleep(10000);
    }
}