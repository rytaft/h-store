package org.qcri.affinityplanner;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

/**
 * @author rytaft
 */
public class TestParallelMigration extends BaseTestCase {

    final int PARTITIONS_PER_SITE = 10;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.B2W);
    }

    public void testCase1() throws Exception {
        Migration migration = new ParallelMigration(PARTITIONS_PER_SITE, 99);
        assertEquals(2, migration.reconfigTime(3, 5));
        assertEquals(10, (int) Math.ceil(migration.moveCost(2, 3, 5)));
        assertEquals(2, migration.reconfigTime(5, 3));
        assertEquals(10, (int) Math.ceil(migration.moveCost(2, 5, 3)));
    }
    
    public void testCase2() throws Exception {
        Migration migration = new ParallelMigration(PARTITIONS_PER_SITE, 89);
        assertEquals(2, migration.reconfigTime(3, 9));
        assertEquals(15, (int) Math.ceil(migration.moveCost(2, 3, 9)));
        assertEquals(2, migration.reconfigTime(9, 3));
        assertEquals(15, (int) Math.ceil(migration.moveCost(2, 9, 3)));
    }
    
    public void testCase3() throws Exception {
        Migration migration = new ParallelMigration(PARTITIONS_PER_SITE, 30 * 14 - 1);
        assertEquals(11, migration.reconfigTime(3, 14));
        assertEquals(3*6 + 3*9 + 2*12 + 3*14, (int) Math.ceil(migration.moveCost(11, 3, 14)));
        assertEquals(11, migration.reconfigTime(14, 3));
        assertEquals(3*6 + 3*9 + 2*12 + 3*14, (int) Math.ceil(migration.moveCost(11, 14, 3)));
    }
    
}
