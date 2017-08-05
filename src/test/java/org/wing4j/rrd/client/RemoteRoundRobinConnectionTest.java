package org.wing4j.rrd.client;

import org.junit.Test;
import org.wing4j.rrd.*;
import org.wing4j.rrd.core.DefaultRoundRobinDatabase;
import org.wing4j.rrd.core.TableMetadata;
import org.wing4j.rrd.core.format.csv.v1.RoundRobinFormatCsvV1;
import org.wing4j.rrd.net.connector.impl.BioRoundRobinConnector;

/**
 * Created by wing4j on 2017/8/2.
 */
public class RemoteRoundRobinConnectionTest {
    @Test
    public void testGetTableMetadata() throws Exception {

        RoundRobinConnection connection = new RemoteRoundRobinConnection(null, new BioRoundRobinConnector(null,  null, "127.0.0.1", 8099), "admin", "password");
//        connection.createTable("test1", "request");
        TableMetadata tableMetadata = connection.getTableMetadata("test1");
        System.out.println(tableMetadata);
        for (int i = 0; i < 10000; i++) {
            long val = connection.increase("test1", "request", 0, 1);
            System.out.println(val);
        }
    }

    @Test
    public void testContain() throws Exception {

    }

    @Test
    public void testIncrease() throws Exception {

    }

    @Test
    public void testIncrease1() throws Exception {

    }

    @Test
    public void testIncrease2() throws Exception {

    }

    @Test
    public void testSlice() throws Exception {

    }

    @Test
    public void testSlice1() throws Exception {

    }

    @Test
    public void testRegisterTrigger() throws Exception {

    }

    @Test
    public void testMerge() throws Exception {

    }

    @Test
    public void testMerge1() throws Exception {

    }

    @Test
    public void testMerge2() throws Exception {

    }

    @Test
    public void testMerge3() throws Exception {

    }

    @Test
    public void testPersistent() throws Exception {

    }

    @Test
    public void testPersistent1() throws Exception {

    }

    @Test
    public void testExpand() throws Exception {

    }

    @Test
    public void testCreateTable() throws Exception {

    }

    @Test
    public void testDropTable() throws Exception {

    }

    @Test
    public void testExecute() throws Exception {

    }

    @Test
    public void testExecuteQuery() throws Exception {

    }

    @Test
    public void testClose() throws Exception {

    }
}