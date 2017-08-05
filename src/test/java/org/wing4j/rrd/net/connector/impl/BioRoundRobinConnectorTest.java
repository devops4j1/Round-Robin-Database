package org.wing4j.rrd.net.connector.impl;

import org.junit.Test;
import org.wing4j.rrd.*;
import org.wing4j.rrd.core.DefaultRoundRobinDatabase;
import org.wing4j.rrd.core.TableMetadata;
import org.wing4j.rrd.core.format.csv.v1.RoundRobinFormatCsvV1;
import org.wing4j.rrd.net.connector.RoundRobinConnector;

/**
 * Created by 面试1 on 2017/8/2.
 */
public class BioRoundRobinConnectorTest {

    @Test
    public void testMerge() throws Exception {
        for (int i = 0; i < 1; i++) {
            long[][] data = new long[][]{
                    {1, 2},
                    {3, 4}
            };
            RoundRobinView view = new RoundRobinView(new String[]{"other1", "response"}, 2, data);
            RoundRobinConnector connector = new BioRoundRobinConnector("127.0.0.1", 8099);
            RoundRobinView newView = connector.merge("mo9", i + 2, view, MergeType.ADD);
            System.out.println(newView);
        }
    }

    @Test
    public void testGetTableMetadata() throws Exception {
        for (int i = 0; i < 1000; i++) {
            RoundRobinConnector connector = new BioRoundRobinConnector("127.0.0.1", 8099);
            TableMetadata metadata = connector.getTableMetadata("mo9");
            System.out.println(metadata);
        }
    }

    @Test
    public void testIncrease() throws Exception {
        for (int i = 0; i < 1000; i++) {
            RoundRobinConnector connector = new BioRoundRobinConnector("127.0.0.1", 8099);
            long v = connector.increase("mo9", "success", 2, 2);
            System.out.println(v);
        }

    }

    @Test
    public void testExpand() throws Exception {
        RoundRobinConnector connector = new BioRoundRobinConnector("127.0.0.1", 8099);
        TableMetadata metadata = connector.expand("mo9", "rep", "other1");
        System.out.println(metadata);
    }

    @Test
    public void testCreateTable() throws Exception {
        RoundRobinConnector connector = new BioRoundRobinConnector("127.0.0.1", 8099);
        connector.createTable("mo9", "success");
    }

    @Test
    public void testSlice() throws Exception {
        for (int i = 0; i < 1; i++) {
            RoundRobinConnector connector = new BioRoundRobinConnector("127.0.0.1", 8099);
            RoundRobinView view = connector.slice(100, 110, "mo9", "success");
            System.out.println(view);
        }

    }
}