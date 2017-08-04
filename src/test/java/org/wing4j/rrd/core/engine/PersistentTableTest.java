package org.wing4j.rrd.core.engine;

import org.junit.Assert;
import org.junit.Test;
import org.wing4j.rrd.FormatType;
import org.wing4j.rrd.MergeType;
import org.wing4j.rrd.RoundRobinResultSet;
import org.wing4j.rrd.RoundRobinView;
import org.wing4j.rrd.core.Table;

/**
 * Created by 面试1 on 2017/8/4.
 */
public class PersistentTableTest {

    @Test
    public void testLock() throws Exception {
        PersistentTable table = new PersistentTable("./target", "mo9", 2, "request", "response");
        table.set(0, "request", 1);
        table.set(1, "request", 2);
        table.set(0, "response", 3);
        table.set(1, "response", 4);
        table.lock();
    }

    @Test
    public void testUnlock() throws Exception {
        PersistentTable table = new PersistentTable("./target", "mo9", 2, "request", "response");
        table.set(0, "request", 1);
        table.set(1, "request", 2);
        table.set(0, "response", 3);
        table.set(1, "response", 4);
        table.unlock();
    }

    @Test
    public void testUnlock1() throws Exception {
        PersistentTable table = new PersistentTable("./target", "mo9", 2, "request", "response");
        table.set(0, "request", 1);
        table.set(1, "request", 2);
        table.set(0, "response", 3);
        table.set(1, "response", 4);
        table.unlock();
        table.unlock();
    }

    @Test
    public void testIncrease() throws Exception {
        PersistentTable table = new PersistentTable("./target", "mo9", 2, "request", "response");
        table.set(0, "request", 1);
        table.set(1, "request", 2);
        table.set(0, "response", 3);
        table.set(1, "response", 4);
        Assert.assertEquals(1, table.get(0, "request"));
        Assert.assertEquals(2, table.get(1, "request"));
        Assert.assertEquals(3, table.get(0, "response"));
        Assert.assertEquals(4, table.get(1, "response"));
        table.increase(1, 0, 1);
        table.increase(1, 1, 2);
        Assert.assertEquals(3, table.get(1, "request"));
        Assert.assertEquals(6, table.get(1, "response"));
    }

    @Test
    public void testIncrease1() throws Exception {
        PersistentTable table = new PersistentTable("./target", "mo9", 2, "request", "response");
        table.set(0, "request", 1);
        table.set(1, "request", 2);
        table.set(0, "response", 3);
        table.set(1, "response", 4);
        Assert.assertEquals(1, table.get(0, "request"));
        Assert.assertEquals(2, table.get(1, "request"));
        Assert.assertEquals(3, table.get(0, "response"));
        Assert.assertEquals(4, table.get(1, "response"));
        table.increase(1, 0, 1);
        table.increase(1, 1, 1);
        table.increase(1, 1, 1);
        Assert.assertEquals(3, table.get(1, "request"));
        Assert.assertEquals(6, table.get(1, "response"));
    }

    @Test
    public void testSlice() throws Exception {
        Table table = new PersistentTable("./target", "mo9", 2, "request", "response");
        table.set(0, "request", 1);
        table.set(1, "request", 2);
        table.set(0, "response", 3);
        table.set(1, "response", 4);
        RoundRobinView view = table.slice(1, 1, "response");
        Assert.assertEquals(4, view.get(0, 0));
    }

    @Test
    public void testSlice1() throws Exception {
        Table table = new PersistentTable("./target", "mo9", 2, "request", "response");
        table.set(0, "request", 1);
        table.set(1, "request", 2);
        table.set(0, "response", 3);
        table.set(1, "response", 4);
        RoundRobinView view = table.slice(2, 1, "response");
        Assert.assertEquals(3, view.get(0, 0));
        Assert.assertEquals(4, view.get(1, 0));
    }

    @Test
    public void testExpand() throws Exception {
        PersistentTable table = new PersistentTable("./target", "mo9", 2, "request", "response");
        table.set(0, "request", 1);
        table.set(1, "request", 2);
        table.set(0, "response", 3);
        table.set(1, "response", 4);
        Assert.assertEquals(2, table.getMetadata().getColumns().length);
        table.expand("success");
        Assert.assertEquals(3, table.getMetadata().getColumns().length);
        Assert.assertEquals(3, table.getData()[0].length);
        Assert.assertEquals("success", table.getMetadata().getColumns()[2]);

    }

    @Test
    public void testMerge() throws Exception {
        PersistentTable table = new PersistentTable("./target", "mo9", 4, "request", "response");
        table.set(0, "request", 1);
        table.set(1, "request", 2);
        table.set(0, "response", 3);
        table.set(1, "response", 4);
        Assert.assertEquals(1, table.get(0, "request"));
        Assert.assertEquals(2, table.get(1, "request"));
        Assert.assertEquals(3, table.get(0, "response"));
        Assert.assertEquals(4, table.get(1, "response"));
        Assert.assertEquals(0, table.get(2, "request"));
        Assert.assertEquals(0, table.get(3, "request"));
        Assert.assertEquals(0, table.get(2, "response"));
        Assert.assertEquals(0, table.get(3, "response"));
        RoundRobinView view = table.slice(2, 1, "request", "response");
        table.merge(view, 3, MergeType.REP);
        Assert.assertEquals(1, table.get(2, "request"));
        Assert.assertEquals(2, table.get(3, "request"));
        Assert.assertEquals(3, table.get(2, "response"));
        Assert.assertEquals(4, table.get(3, "response"));
    }

    @Test
    public void testMerge2() throws Exception {
        PersistentTable table = new PersistentTable("./target", "mo9", 4, "request", "response");
        table.set(0, "request", 1);
        table.set(1, "request", 2);
        table.set(0, "response", 3);
        table.set(1, "response", 4);
        Assert.assertEquals(1, table.get(0, "request"));
        Assert.assertEquals(2, table.get(1, "request"));
        Assert.assertEquals(3, table.get(0, "response"));
        Assert.assertEquals(4, table.get(1, "response"));
        Assert.assertEquals(0, table.get(2, "request"));
        Assert.assertEquals(0, table.get(3, "request"));
        Assert.assertEquals(0, table.get(2, "response"));
        Assert.assertEquals(0, table.get(3, "response"));
        RoundRobinView view = table.slice(2, 1, "request", "response");
        table.merge(view, 3, MergeType.REP);
        table.merge(view, 3, MergeType.REP);
        Assert.assertEquals(1, table.get(2, "request"));
        Assert.assertEquals(2, table.get(3, "request"));
        Assert.assertEquals(3, table.get(2, "response"));
        Assert.assertEquals(4, table.get(3, "response"));
    }

    @Test
    public void testMerge3() throws Exception {
        PersistentTable table = new PersistentTable("./target", "mo9", 4, "request", "response");
        table.set(0, "request", 1);
        table.set(1, "request", 2);
        table.set(0, "response", 3);
        table.set(1, "response", 4);
        Assert.assertEquals(1, table.get(0, "request"));
        Assert.assertEquals(2, table.get(1, "request"));
        Assert.assertEquals(3, table.get(0, "response"));
        Assert.assertEquals(4, table.get(1, "response"));
        Assert.assertEquals(0, table.get(2, "request"));
        Assert.assertEquals(0, table.get(3, "request"));
        Assert.assertEquals(0, table.get(2, "response"));
        Assert.assertEquals(0, table.get(3, "response"));
        RoundRobinView view = table.slice(2, 1, "request", "response");
        table.merge(view, 3, MergeType.ADD);
        Assert.assertEquals(1, table.get(2, "request"));
        Assert.assertEquals(2, table.get(3, "request"));
        Assert.assertEquals(3, table.get(2, "response"));
        Assert.assertEquals(4, table.get(3, "response"));
        table.merge(view, 3, MergeType.ADD);
        Assert.assertEquals(2, table.get(2, "request"));
        Assert.assertEquals(4, table.get(3, "request"));
        Assert.assertEquals(6, table.get(2, "response"));
        Assert.assertEquals(8, table.get(3, "response"));
    }

    @Test
    public void testPersistent() throws Exception {
        Table table = new PersistentTable("./target", "mo9", 2, "request", "response");
        table.set(0, "request", 1);
        table.set(1, "request", 2);
        table.set(0, "response", 3);
        table.set(1, "response", 4);
        table.persistent();
    }

    @Test
    public void testPersistent1() throws Exception {
        Table table = new PersistentTable("./target", "mo9", 2, "request", "response");
        table.set(0, "request", 1);
        table.set(1, "request", 2);
        table.set(0, "response", 3);
        table.set(1, "response", 4);
        table.persistent(FormatType.CSV, 1);
    }

    @Test
    public void testGet() throws Exception {
        Table table = new PersistentTable("./target", "mo9", 2, "request", "response");
        table.set(0, "request", 1);
        table.set(1, "request", 2);
        table.set(0, "response", 3);
        table.set(1, "response", 4);
        Assert.assertEquals(1, table.get(0, "request"));
        Assert.assertEquals(2, table.get(1, "request"));
        Assert.assertEquals(3, table.get(0, "response"));
        Assert.assertEquals(4, table.get(1, "response"));
    }
}