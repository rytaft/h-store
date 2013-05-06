/**
 * 
 */
package edu.brown.hstore;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.ycsb.YCSBConstants;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;

/**
 * @author aelmore
 *
 */
public class TestReconfigurationEE extends BaseTestCase {
    
    private static final int NUM_PARTITIONS = 1;
    private static final int NUM_TUPLES = 1000;
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
    
    private void loadData() throws Exception {
        // Load in a bunch of dummy data for this table
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        assertNotNull(vt);
        for (int i = 0; i < NUM_TUPLES; i++) {
            Object row[] = VoltTableUtil.getRandomRow(catalog_tbl);
            row[0] = i;
            vt.addRow(row);
        } // FOR
        this.executor.loadTable(1000l, catalog_tbl, vt, false);

    }
    
    @Test
    public void testLoadData() throws Exception {
        this.loadData();
    	System.out.println("Test load data");
    	assertTrue(true);
    	System.out.println("Test load data 2");

    	this.ee.extractTable(1, this.catalog_tbl.getRelativeIndex(), new byte[10], 1, 1, 1);
    	System.out.println("Test load data 3");
    	assertTrue(true);
    	System.out.println("End Test load data");
    	
    }
}
