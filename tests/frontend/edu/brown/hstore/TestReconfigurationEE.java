/**
 * 
 */
package edu.brown.hstore;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.ycsb.YCSBConstants;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.reconfiguration.ReconfigurationUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

/**
 * @author aelmore
 *
 */
public class TestReconfigurationEE extends BaseTestCase {

    private static final Logger LOG = Logger.getLogger(TestReconfigurationEE.class);
    private static final int NUM_PARTITIONS = 1;
    private static final long NUM_TUPLES = 1000;
    private static final String TARGET_TABLE = YCSBConstants.TABLE_NAME;

    
    private HStoreSite hstore_site;
    private HStoreConf hstore_conf;
    private Client client;
    
    private PartitionExecutor executor;
    private ExecutionEngine ee;
    private Table catalog_tbl;
    
    //private int ycsbTableId(Catalog catalog) {
    //    return catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get(TARGET_TABLE).getRelativeIndex();
    // }
    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.YCSB);
        initializeCatalog(1, 1, NUM_PARTITIONS);
        
        // Just make sure that the Table has the evictable flag set to true
        this.catalog_tbl = getTable(TARGET_TABLE);
        
        
        Site catalog_site = CollectionUtil.first(CatalogUtil.getCluster(catalog).getSites());
        this.hstore_conf = HStoreConf.singleton();
        this.hstore_conf.site.status_enable = false;

        
        this.hstore_site = createHStoreSite(catalog_site, hstore_conf);
        this.executor = hstore_site.getPartitionExecutor(0);
        assertNotNull(this.executor);
        this.ee = executor.getExecutionEngine();
        assertNotNull(this.executor);
        
        this.client = createClient();
    }
    
    @Override
    protected void tearDown() throws Exception {
        if (this.client != null) this.client.close();
        if (this.hstore_site != null) this.hstore_site.shutdown();
    }
    
    private void loadData(Long numTuples) throws Exception {
        // Load in a bunch of dummy data for this table
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        assertNotNull(vt);
        for (int i = 0; i < numTuples; i++) {
            Object row[] = VoltTableUtil.getRandomRow(catalog_tbl);
            row[0] = i;
            vt.addRow(row);
        } // FOR
        this.executor.loadTable(1000L, catalog_tbl, vt, false);

    }
    
    @Test
    public void testExtractDataLarge() throws Exception {

        long tuples= 9000;
        this.loadData(tuples);
        assertTrue(true);
        ReconfigurationRange<Long> range = new ReconfigurationRange<Long>("usertable", VoltType.BIGINT, new Long(0), tuples, 1, 2);
        VoltTable extractTable = ReconfigurationUtil.getExtractVoltTable(range);        
        VoltTable resTable= this.ee.extractTable(this.catalog_tbl.getRelativeIndex(), extractTable, 1, 1, 1);       
        assertEquals(tuples,resTable.getRowCount());
    }
    
    @Test
    public void tesatExtractData() throws Exception {

        
        this.loadData(NUM_TUPLES);
    	assertTrue(true);
    	ReconfigurationRange<Long> range = new ReconfigurationRange<Long>("usertable", VoltType.BIGINT, new Long(100), new Long(102), 1, 2);
    	VoltTable extractTable = ReconfigurationUtil.getExtractVoltTable(range);        
    	VoltTable resTable= this.ee.extractTable(this.catalog_tbl.getRelativeIndex(), extractTable, 1, 1, 1);    	
        assertTrue(resTable.getRowCount()==2);
    	LOG.info("Results : " + resTable.toString(true));  	
    	resTable= this.ee.extractTable(this.catalog_tbl.getRelativeIndex(), extractTable, 1, 1, 1);
    	assertTrue(resTable.getRowCount()==0);
    	
    	range = new ReconfigurationRange<Long>("usertable", VoltType.BIGINT, new Long(998), new Long(1002), 1, 2);
        extractTable = ReconfigurationUtil.getExtractVoltTable(range);        
        resTable= this.ee.extractTable(this.catalog_tbl.getRelativeIndex(), extractTable, 1, 1, 1);       
        assertTrue(resTable.getRowCount()==2);
        
        range = new ReconfigurationRange<Long>("usertable", VoltType.BIGINT, new Long(995), new Long(998), 1, 2);
        extractTable = ReconfigurationUtil.getExtractVoltTable(range);        
        resTable= this.ee.extractTable(this.catalog_tbl.getRelativeIndex(), extractTable, 1, 1, 1);       
        assertTrue(resTable.getRowCount()==3);
        
        range = new ReconfigurationRange<Long>("usertable", VoltType.BIGINT, new Long(200), new Long(300), 1, 2);
        extractTable = ReconfigurationUtil.getExtractVoltTable(range);        
        resTable= this.ee.extractTable(this.catalog_tbl.getRelativeIndex(), extractTable, 1, 1, 1);       
        assertTrue(resTable.getRowCount()==100);
        
        range = new ReconfigurationRange<Long>("usertable", VoltType.BIGINT, new Long(300), new Long(300), 1, 2);
        extractTable = ReconfigurationUtil.getExtractVoltTable(range);        
        resTable= this.ee.extractTable(this.catalog_tbl.getRelativeIndex(), extractTable, 1, 1, 1);       
        assertTrue(resTable.getRowCount()==1);
        
        range = new ReconfigurationRange<Long>("usertable", VoltType.BIGINT, new Long(301), new Long(302), 1, 2);
        extractTable = ReconfigurationUtil.getExtractVoltTable(range);        
        resTable= this.ee.extractTable(this.catalog_tbl.getRelativeIndex(), extractTable, 1, 1, 1);       
        assertTrue(resTable.getRowCount()==1);
        
        //TODO ae andy we don't want to crash the system. need to find right exception
        /*
        boolean excCaught = false;
        try{
            range = new ReconfigurationRange<Long>("usertable", VoltType.BIGINT, new Long(301), new Long(300), 1, 2);
            extractTable = ReconfigurationUtil.getExtractVoltTable(range);        
            resTable= this.ee.extractTable(this.catalog_tbl.getRelativeIndex(), extractTable, 1, 1, 1);      
            
        }
        catch(Exception ex){
            excCaught=true;
        }
        assertTrue(excCaught);
        */
        
    }
}
