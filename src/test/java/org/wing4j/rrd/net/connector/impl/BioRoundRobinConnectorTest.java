package org.wing4j.rrd.net.connector.impl;

import com.google.gson.Gson;
import org.junit.Test;
import org.wing4j.rrd.*;
import org.wing4j.rrd.core.DefaultRoundRobinDatabase;
import org.wing4j.rrd.core.format.csv.v1.RoundRobinFormatCsvV1;
import org.wing4j.rrd.net.connector.RoundRobinConnector;

import static org.junit.Assert.*;

/**
 * Created by 面试1 on 2017/8/2.
 */
public class BioRoundRobinConnectorTest {

    @Test
    public void testWrite() throws Exception {
//        String[] header = {"req", "rsp"};
//        int[] timeline = {1, 2};
//        long[][] data = {{1L, 2L}, {3L, 4L}};
//        RoundRobinView view = new RoundRobinView(header, timeline, data, 1);

        RoundRobinDatabase database = DefaultRoundRobinDatabase.init(new RoundRobinConfig());
        RoundRobinConnection connection = database.open("D:/2.rrd");
        RoundRobinView view = connection.slice(60 * 60, connection.getHeader());
        RoundRobinFormat format = new RoundRobinFormatCsvV1(view);
        format.write("D:/22.csv");
        for (int i = 0; i < 1; i++) {
            RoundRobinConnector connector = new BioRoundRobinConnector("127.0.0.1", 8099);
            connector.write(view, 0, MergeType.ADD);
        }
    }

    @Test
    public void testStart() throws Exception {

    }
}