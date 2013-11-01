/**
 * 
 */
package edu.brown.hstore;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.ycsb.YCSBConstants;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hashing.PlannedHasher;
import edu.brown.hashing.PlannedPartitions.PartitionRange;
import edu.brown.hashing.PlannedPartitions.PartitionedTable;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationTable;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.reconfiguration.ReconfigurationUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

/**
 * @author aelmore
 *
 */
public class TestReconfigurationExtractImportEE extends BaseTestCase {

    private static final Logger LOG = Logger.getLogger(TestReconfigurationExtractImportEE.class);
    private static final int NUM_PARTITIONS = 1;
    private static final long NUM_TUPLES = 1000;
    private static final String CUSTOMER_TABLE_NAME = TPCCConstants.TABLENAME_CUSTOMER;
    private static final String NEW_ORDER_TABLE_NAME = TPCCConstants.TABLENAME_NEW_ORDER;

    
    private HStoreSite hstore_site;
    private HStoreConf hstore_conf;
    private Client client;
    
    private PartitionExecutor executor;
    private ExecutionEngine ee;
    private Table customer_tbl;
    private Table neworder_tbl;
    private int neworder_p_index;
    private int cust_p_index;
    
    //private int ycsbTableId(Catalog catalog) {
    //    return catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get(TARGET_TABLE).getRelativeIndex();
    // }
    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        initializeCatalog(1, 1, NUM_PARTITIONS);
        
        // Just make sure that the Table has the evictable flag set to true
        this.customer_tbl = getTable(CUSTOMER_TABLE_NAME);
        this.cust_p_index = this.customer_tbl.getPartitioncolumn().getIndex();
        this.neworder_tbl = getTable(NEW_ORDER_TABLE_NAME);
        this.neworder_p_index = this.neworder_tbl.getPartitioncolumn().getIndex();
        
        Site catalog_site = CollectionUtil.first(CatalogUtil.getCluster(catalog).getSites());
        hstore_conf = HStoreConf.singleton();
        
        hstore_conf.site.coordinator_sync_time = false;
        hstore_conf.global.reconfiguration_enable = true;
        //hstore_conf.global.hasher_class = "edu.brown.hashing.PlannedHasher";
        //hstore_conf.global.hasher_plan = PlannedHasher.YCSB_TEST;
        
        hstore_conf.site.status_enable = false;

        
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
    
    private void loadTPCCData(Long numTuples, Table table, int widIndex, int wid ) throws Exception {
        // Load in a bunch of dummy data for this table
        VoltTable vt = CatalogUtil.getVoltTable(table);
        assertNotNull(vt);
        for (int i = 0; i < numTuples; i++) {
            Object row[] = VoltTableUtil.getRandomRow(table);
            row[0] = i;
            row[widIndex] = wid;
            vt.addRow(row);
        } // FOR
        this.executor.loadTable(1000L, table, vt, false);

    }
    

    
    @Test
    public void testExtractData() throws Exception {

        
        this.loadTPCCData(NUM_TUPLES, this.customer_tbl,this.cust_p_index,1);
        this.loadTPCCData(NUM_TUPLES, this.neworder_tbl,this.neworder_p_index,1);

        
        this.loadTPCCData(NUM_TUPLES*2, this.customer_tbl,this.cust_p_index,2);
        this.loadTPCCData(NUM_TUPLES*4, this.customer_tbl,this.cust_p_index,4);

        this.loadTPCCData(NUM_TUPLES*2, this.neworder_tbl,this.neworder_p_index,2);
        this.loadTPCCData(NUM_TUPLES*4, this.neworder_tbl,this.neworder_p_index,4);
        
        
    	assertTrue(true);
    	
    	
    	ReconfigurationRange<Long> range = new ReconfigurationRange<Long>(this.customer_tbl.getName(), VoltType.SMALLINT, new Long(1), new Long(2), 1, 2);
    	VoltTable extractTable = ReconfigurationUtil.getExtractVoltTable(range);   
    	long start = System.currentTimeMillis();
    	VoltTable resTable= this.ee.extractTable(this.customer_tbl.getRelativeIndex(), extractTable, 1, 1, 1,executor.getNextRequestToken()); 
        assertTrue(resTable.getRowCount()==NUM_TUPLES);

    
        long extract = System.currentTimeMillis();
        
        
    	assertTrue(resTable.getRowCount()==NUM_TUPLES);
    	//LOG.info("Results : " + resTable.toString(true));  	
    	resTable= this.ee.extractTable(this.customer_tbl.getRelativeIndex(), extractTable, 1, 1, 1, executor.getNextRequestToken());
    	assertTrue(resTable.getRowCount()==0);
    	
    	start = System.currentTimeMillis();
        this.executor.loadTable(2000L, this.customer_tbl, extractTable, false);
        long load = System.currentTimeMillis();
        
        LOG.info(String.format("Extract took :%s Load Took %s ", extract, load));
    	
    	/*
    	range = new ReconfigurationRange<Long>("usertable", VoltType.BIGINT, new Long(998), new Long(1002), 1, 2);
        extractTable = ReconfigurationUtil.getExtractVoltTable(range);        
        resTable= this.ee.extractTable(this.customer_tbl.getRelativeIndex(), extractTable, 1, 1, 1, executor.getNextRequestToken());       
        assertTrue(resTable.getRowCount()==2);
        */

    }
}
