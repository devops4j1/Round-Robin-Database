package org.wing4j.rrd.client;

import org.junit.Test;
import org.wing4j.rrd.*;
import org.wing4j.rrd.core.DefaultRoundRobinDatabase;
import org.wing4j.rrd.core.format.csv.v1.RoundRobinFormatCsvV1;
import org.wing4j.rrd.net.connector.RoundRobinConnector;
import org.wing4j.rrd.net.connector.impl.AioRoundRobinConnector;

import static org.junit.Assert.*;

/**
 * Created by wing4j on 2017/8/2.
 */
public class RemoteRoundRobinConnectionTest {

    @Test
    public void testMerge() throws Exception {
        RoundRobinConfig config = new RoundRobinConfig();
        RoundRobinDatabase database = DefaultRoundRobinDatabase.init(config);
        RoundRobinConnection connection = database.open("D:/2.rrd");
        RoundRobinView view = connection.slice(1 * 60, connection.getHeader());
        RoundRobinFormat format = new RoundRobinFormatCsvV1(view);
        format.write("D:/22.csv");
        RoundRobinConnection remoteConnection =  database.open("127.0.0.1", 8099);
        remoteConnection.merge(view, MergeType.ADD);
        Thread.sleep(1000);
    }
}