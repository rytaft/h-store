/**
 * 
 */
package edu.brown.hstore;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.client.Client;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngineJNI;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.benchmark.ycsb.YCSBConstants;
import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.MemoryEstimator;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.hashing.PlannedHasher;
import edu.brown.hashing.TwoTieredRangeHasher;
import edu.brown.hashing.PlannedPartitions.PartitionRange;
import edu.brown.hashing.PlannedPartitions.PartitionedTable;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationTable;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.reconfiguration.ReconfigurationUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

/**
 * @author rytaft
 */
public class TestReconfigurationMultiPartitionEE extends BaseTestCase {

    private static final Logger LOG = Logger.getLogger(TestReconfigurationMultiPartitionEE.class);
    private static final int NUM_PARTITIONS = 1;
    private static final long NUM_TUPLES = 100;
    private static final String CUSTOMER_TABLE_NAME = TPCCConstants.TABLENAME_CUSTOMER;
    private static final String NEW_ORDER_TABLE_NAME = TPCCConstants.TABLENAME_NEW_ORDER;
    private static final int DEFAULT_LIMIT = ExecutionEngineJNI.DEFAULT_EXTRACT_LIMIT_BYTES;
    
    private HStoreSite hstore_site;
    private HStoreConf hstore_conf;
    private Client client;

    private PartitionExecutor executor;
    private ExecutionEngine ee;
    //private Table catalog_tbl;
    private Table customer_tbl;
    private Table neworder_tbl;
    private ArrayList<Integer> neworder_p_index;
    private ArrayList<Integer> cust_p_index;
    private int undo=1;

    // private int ycsbTableId(Catalog catalog) {
    // return
    // catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get(TARGET_TABLE).getRelativeIndex();
    // }
    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        
    	JSONObject json = new JSONObject(partitionPlan);
    	
    	PartitionPlan pplan = new PartitionPlan();
        pplan.fromJSON(json, this.catalog_db);

        // Apply!
        boolean secondaryIndexes = false;
        LOG.info(String.format("Applying PartitionPlan to catalog [enableSecondaryIndexes=%s]", secondaryIndexes));
        pplan.apply(this.catalog_db, secondaryIndexes);
        
        initializeCatalog(1, 1, NUM_PARTITIONS);
        
        // Just make sure that the Table has the evictable flag set to true
        this.customer_tbl = getTable(CUSTOMER_TABLE_NAME);
        this.cust_p_index = new ArrayList<Integer>(this.customer_tbl.getPartitioncolumns().size());
        for(ColumnRef colRef : this.customer_tbl.getPartitioncolumns().values()) {
        	this.cust_p_index.set(colRef.getIndex(), colRef.getColumn().getIndex());
    	}
        
        this.neworder_tbl = getTable(NEW_ORDER_TABLE_NAME);
        this.neworder_p_index = new ArrayList<Integer>(this.neworder_tbl.getPartitioncolumns().size());
        for(ColumnRef colRef : this.neworder_tbl.getPartitioncolumns().values()) {
        	this.neworder_p_index.set(colRef.getIndex(), colRef.getColumn().getIndex());
    	}
        
        Site catalog_site = CollectionUtil.first(CatalogUtil.getCluster(catalog).getSites());
        hstore_conf = HStoreConf.singleton();
        
        hstore_conf.site.coordinator_sync_time = false;
        hstore_conf.global.reconfiguration_enable = true;
        hstore_conf.global.hasher_class = "edu.brown.hashing.PlannedHasher";
        hstore_conf.global.hasher_plan = PlannedHasher.TPCC_TEST;

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
        if (this.client != null)
            this.client.close();
        if (this.hstore_site != null)
            this.hstore_site.shutdown();
    }

    private void loadTPCCData(Long numTuples, Table table, List<Integer> keyIndexes, List<Integer> keys ) throws Exception {
    	// Load in a bunch of dummy data for this table
        VoltTable vt = CatalogUtil.getVoltTable(table);
        assertNotNull(vt);
        for (int i = 0; i < numTuples; i++) {
            Object row[] = VoltTableUtil.getRandomRow(table);
            row[0] = i;
            Iterator<Integer> it = keyIndexes.iterator();
            Iterator<Integer> itk = keys.iterator();
            while(it.hasNext() && itk.hasNext()) {
            	row[it.next()] = itk.next();
            }
            vt.addRow(row);
        } // FOR
        this.executor.loadTable(1000L, table, vt, false);

    }
    
    String partitionPlan = "{" +
"    		 \"TABLE_ENTRIES\": {" +
"    	  \"{'database':'CUSTOMER'}\": {" +
"    	   \"PARENT_ATTRIBUTE\": \"{'DISTRICT':'D_W_ID'}\"," +
"    	   \"PARENT\": \"{'database':'DISTRICT'}\"," +
"    	   \"METHOD\": \"HASH\"," +
"    	   \"ATTRIBUTE\": \"{'CUSTOMER#*MultiColumn*':[{'CUSTOMER':'C_W_ID'},{'CUSTOMER':'C_D_ID'}]}\"," +
"    	   \"ATTRIBUTE_class\": \"edu.brown.catalog.special.MultiColumn\"" +
"    	  }," +
"    	  \"{'database':'CUSTOMER_NAME'}\": {" +
"    	   \"PARENT_ATTRIBUTE\": null," +
"    	   \"PARENT\": null," +
"    	   \"METHOD\": \"REPLICATION\"," +
"    	   \"ATTRIBUTE\": null" +
"    	  }," +
"    	  \"{'database':'DISTRICT'}\": {" +
"    	   \"PARENT_ATTRIBUTE\": null," +
"    	   \"PARENT\": null," +
"    	   \"METHOD\": \"HASH\"," +
"    	   \"ATTRIBUTE\": \"{'DISTRICT#*MultiColumn*':[{'DISTRICT':'D_W_ID'},{'DISTRICT':'D_ID'}]}\"," +
"    	   \"ATTRIBUTE_class\": \"edu.brown.catalog.special.MultiColumn\"" +
"    	  }," +
"    	  \"{'database':'HISTORY'}\": {" +
"    	   \"PARENT_ATTRIBUTE\": \"{'DISTRICT':'D_ID'}\"," +
"    	   \"PARENT\": \"{'database':'DISTRICT'}\"," +
"    	   \"METHOD\": \"MAP\"," +
"    	   \"ATTRIBUTE\": \"{'HISTORY#*MultiColumn*':[{'HISTORY':'H_W_ID'},{'HISTORY':'H_D_ID'}]}\"," +
"    	   \"ATTRIBUTE_class\": \"edu.brown.catalog.special.MultiColumn\"" +
"    	  }," +
"    	  \"{'database':'ITEM'}\": {" +
"    	   \"PARENT_ATTRIBUTE\": null," +
"    	   \"PARENT\": null," +
"    	   \"METHOD\": \"REPLICATION\"," +
"    	   \"ATTRIBUTE\": null" +
"    	  }," +
"    	  \"{'database':'NEW_ORDER'}\": {" +
"    	   \"PARENT_ATTRIBUTE\": null," +
"    	   \"PARENT\": null," +
"    	   \"METHOD\": \"HASH\"," +
"    	   \"ATTRIBUTE\": \"{'NEW_ORDER#*MultiColumn*':[{'NEW_ORDER':'NO_W_ID'},{'NEW_ORDER':'NO_D_ID'}]}\"," +
"    	   \"ATTRIBUTE_class\": \"edu.brown.catalog.special.MultiColumn\"" +
"    	  }," +
"    	  \"{'database':'ORDERS'}\": {" +
"    	   \"PARENT_ATTRIBUTE\": null," +
"    	   \"PARENT\": null," +
"    	   \"METHOD\": \"HASH\"," +
"    	   \"ATTRIBUTE\": \"{'ORDERS#*MultiColumn*':[{'ORDERS':'O_W_ID'},{'ORDERS':'O_D_ID'}]}\"," +
"    	   \"ATTRIBUTE_class\": \"edu.brown.catalog.special.MultiColumn\"" +
"    	  }," +
"    	  \"{'database':'ORDER_LINE'}\": {" +
"    	   \"PARENT_ATTRIBUTE\": null," +
"    	   \"PARENT\": null," +
"    	   \"METHOD\": \"HASH\"," +
"    	   \"ATTRIBUTE\": \"{'ORDER_LINE#*MultiColumn*':[{'ORDER_LINE':'OL_W_ID'},{'ORDER_LINE':'OL_D_ID'}]}\"," +
"    	   \"ATTRIBUTE_class\": \"edu.brown.catalog.special.MultiColumn\"" +
"    	  }," +
"    	  \"{'database':'STOCK'}\": {" +
"    	   \"PARENT_ATTRIBUTE\": null," +
"    	   \"PARENT\": null," +
"    	   \"METHOD\": \"HASH\"," +
"    	   \"ATTRIBUTE\": \"{'STOCK#*MultiColumn*':[{'STOCK':'S_I_ID'},{'STOCK':'S_W_ID'}]}\"," +
"    	   \"ATTRIBUTE_class\": \"edu.brown.catalog.special.MultiColumn\"" +
"    	  }," +
"    	  \"{'database':'WAREHOUSE'}\": {" +
"    	   \"PARENT_ATTRIBUTE\": null," +
"    	   \"PARENT\": null," +
"    	   \"METHOD\": \"REPLICATION\"," +
"    	   \"ATTRIBUTE\": null" +
"    	  }" +
"    	 }," +
"    	 \"PROC_ENTRIES\": {" +
"    	  \"{'database':'delivery'}\": {" +
"    	   \"SINGLE_PARTITION\": true," +
"    	   \"METHOD\": \"HASH\"," +
"    	   \"ATTRIBUTE\": \"{'delivery':'0'}\"," +
"    	   \"ATTRIBUTE_class\": \"org.voltdb.catalog.ProcParameter\"" +
"    	  }," +
"    	  \"{'database':'neworder'}\": {" +
"    	   \"SINGLE_PARTITION\": false," +
"    	   \"METHOD\": \"HASH\"," +
"    	   \"ATTRIBUTE\": \"{'neworder#*MultiProcParameter*':[{'neworder':'0'},{'neworder':'1'}]}\"," +
"    	   \"ATTRIBUTE_class\": \"edu.brown.catalog.special.MultiProcParameter\"" +
"    	  }," +
"    	  \"{'database':'ostatByCustomerId'}\": {" +
"    	   \"SINGLE_PARTITION\": true," +
"    	   \"METHOD\": \"HASH\"," +
"    	   \"ATTRIBUTE\": \"{'ostatByCustomerId#*MultiProcParameter*':[{'ostatByCustomerId':'0'},{'ostatByCustomerId':'1'}]}\"," +
"    	   \"ATTRIBUTE_class\": \"edu.brown.catalog.special.MultiProcParameter\"" +
"    	  }," +
"    	  \"{'database':'ostatByCustomerName'}\": {" +
"    	   \"SINGLE_PARTITION\": true," +
"    	   \"METHOD\": \"HASH\"," +
"    	   \"ATTRIBUTE\": \"{'ostatByCustomerName#*MultiProcParameter*':[{'ostatByCustomerName':'0'},{'ostatByCustomerName':'1'}]}\"," +
"    	   \"ATTRIBUTE_class\": \"edu.brown.catalog.special.MultiProcParameter\"" +
"    	  }," +
"    	  \"{'database':'paymentByCustomerId'}\": {" +
"    	   \"SINGLE_PARTITION\": false," +
"    	   \"METHOD\": \"HASH\"," +
"    	   \"ATTRIBUTE\": \"{'paymentByCustomerId#*MultiProcParameter*':[{'paymentByCustomerId':'0'},{'paymentByCustomerId':'1'}]}\"," +
"    	   \"ATTRIBUTE_class\": \"edu.brown.catalog.special.MultiProcParameter\"" +
"    	  }," +
"    	  \"{'database':'paymentByCustomerName'}\": {" +
"    	   \"SINGLE_PARTITION\": false," +
"    	   \"METHOD\": \"HASH\"," +
"    	   \"ATTRIBUTE\": \"{'paymentByCustomerName#*MultiProcParameter*':[{'paymentByCustomerName':'0'},{'paymentByCustomerName':'1'}]}\"," +
"    	   \"ATTRIBUTE_class\": \"edu.brown.catalog.special.MultiProcParameter\"" +
"    	  }," +
"    	  \"{'database':'slev'}\": {" +
"    	   \"SINGLE_PARTITION\": true," +
"    	   \"METHOD\": \"HASH\"," +
"    	   \"ATTRIBUTE\": \"{'slev#*MultiProcParameter*':[{'slev':'0'},{'slev':'1'}]}\"," +
"    	   \"ATTRIBUTE_class\": \"edu.brown.catalog.special.MultiProcParameter\"" +
"    	  }" +
"    	 }" +
"    	}";
    
    int[] scales = { 1,2,3,5 };
    
    @Test
    public void testSingleColumnRangeExtractAll() throws Exception {  
    	int wid = 2; // warehouse id
    	ArrayList<Integer> keys = new ArrayList<Integer>();
        keys.add(wid); 
    	
    	ReconfigurationRange<Long> range; 
        VoltTable extractTable;
        range = new ReconfigurationRange<Long>(this.customer_tbl.getName(), VoltType.SMALLINT, new Long(wid), new Long(wid+1), 1, 2);
        extractTable = ReconfigurationUtil.getExtractVoltTable(range);   
        this.loadTPCCData(NUM_TUPLES * 10, this.customer_tbl,this.cust_p_index, keys);
        int EXTRACT_LIMIT = 2048;
        ((ExecutionEngineJNI)(this.ee)).DEFAULT_EXTRACT_LIMIT_BYTES = EXTRACT_LIMIT;
        
        long tupleBytes = MemoryEstimator.estimateTupleSize(this.customer_tbl);
        int tuplesInChunk = (int)(EXTRACT_LIMIT / tupleBytes);
        int expectedChunks = ((int)(NUM_TUPLES * 10)/tuplesInChunk);
        int resCount = 0;
        int chunks = 0;
        Pair<VoltTable,Boolean> resTable = 
                this.ee.extractTable(this.customer_tbl, this.customer_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
        assertTrue(resTable.getSecond());
        resCount += resTable.getFirst().getRowCount();
        chunks++;
        while(resTable.getSecond()){
            resTable = 
                    this.ee.extractTable(this.customer_tbl, this.customer_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
            resCount += resTable.getFirst().getRowCount();
            chunks++;
        }
        assertEquals(expectedChunks, chunks);
        assertEquals(NUM_TUPLES*10, resCount);
    }
    
    @Test
    public void testSingleColumnRange() throws Exception {  
    	((ExecutionEngineJNI)(this.ee)).DEFAULT_EXTRACT_LIMIT_BYTES = DEFAULT_LIMIT;

        for (int i=0; i< scales.length; i++) {
        	ArrayList<Integer> keys = new ArrayList<Integer>();
            keys.add(scales[i]); 
        	
        	long tuples = NUM_TUPLES * scales[i];
            LOG.info(String.format("Loading %s tuples for customers with W_ID :%s ", tuples,scales[i]));
            this.loadTPCCData(tuples, this.customer_tbl,this.cust_p_index,keys);
                
        }
        LOG.info("load done");
       
    	assertTrue(true);
    	
    	ReconfigurationRange<Long> range; 
        VoltTable extractTable;
        long start, extract, load;
    	Pair<VoltTable,Boolean> resTable;
    	Pair<VoltTable,Boolean> resTableVerify;
        
    	
    	for (int i=0; i< scales.length; i++) {
    	    int scale = scales[i];
    	    LOG.info("Testing for scale : " + scale);
    	    //extract
            range = new ReconfigurationRange<Long>(this.customer_tbl.getName(), VoltType.SMALLINT, new Long(scale), new Long(scale+1), 1, 2);
            extractTable = ReconfigurationUtil.getExtractVoltTable(range);   
            start = System.currentTimeMillis();
            resTable= this.ee.extractTable(this.customer_tbl, this.customer_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
            extract = System.currentTimeMillis()-start; 
            assertFalse(resTable.getSecond());
            LOG.info("Tuples : " + resTable.getFirst().getRowCount());
            assertTrue(resTable.getFirst().getRowCount()==NUM_TUPLES *scale);
            
            //assert empty     
            resTableVerify= this.ee.extractTable(this.customer_tbl, this.customer_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
            assertTrue(resTableVerify.getFirst().getRowCount()==0);
            
            
            //load
            start = System.currentTimeMillis();
            this.executor.loadTable(2000L, this.customer_tbl, resTable.getFirst(), false);
            load = System.currentTimeMillis() - start;
            LOG.info(String.format("size=%s Extract=%s Load=%s Diff:%s", NUM_TUPLES*scale, extract, load, load-extract));

            //re extract and check its there
            resTableVerify= this.ee.extractTable(this.customer_tbl, this.customer_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
            assertTrue(resTableVerify.getFirst().getRowCount()==NUM_TUPLES *scale);
    	}

    }
    
    @Test
    public void testTwoColumnRangeExtractAll() throws Exception {
    	int wid = 2; // warehouse id
    	int did = 3; // district id
    	ArrayList<Integer> keys = new ArrayList<Integer>();
        keys.add(wid); 
    	keys.add(did); 
        
    	ReconfigurationRange<Long> districtRange = new ReconfigurationRange<Long>(this.customer_tbl.getName(), this.customer_tbl.getColumns().get(this.cust_p_index.get(1)).getName(),
    			VoltType.SMALLINT, new Long(did), new Long(did+1), 1, 2, null);
        ReconfigurationRange<Long> range = new ReconfigurationRange<Long>(this.customer_tbl.getName(), this.customer_tbl.getColumns().get(this.cust_p_index.get(0)).getName(),
        		VoltType.SMALLINT, new Long(wid), new Long(wid+1), 1, 2, districtRange);
        VoltTable extractTable = ReconfigurationUtil.getExtractVoltTable(range);   
        this.loadTPCCData(NUM_TUPLES * 10, this.customer_tbl,this.cust_p_index, keys);
        int EXTRACT_LIMIT = 2048;
        ((ExecutionEngineJNI)(this.ee)).DEFAULT_EXTRACT_LIMIT_BYTES = EXTRACT_LIMIT;
        
        long tupleBytes = MemoryEstimator.estimateTupleSize(this.customer_tbl);
        int tuplesInChunk = (int)(EXTRACT_LIMIT / tupleBytes);
        int expectedChunks = ((int)(NUM_TUPLES * 10)/tuplesInChunk);
        int resCount = 0;
        int chunks = 0;
        Pair<VoltTable,Boolean> resTable = 
                this.ee.extractTable(this.customer_tbl, this.customer_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
        assertTrue(resTable.getSecond());
        resCount += resTable.getFirst().getRowCount();
        chunks++;
        while(resTable.getSecond()){
            resTable = 
                    this.ee.extractTable(this.customer_tbl, this.customer_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
            resCount += resTable.getFirst().getRowCount();
            chunks++;
        }
        assertEquals(expectedChunks, chunks);
        assertEquals(NUM_TUPLES*10, resCount);
    }
    
    @Test
    public void testTwoColumnRange() throws Exception {
    	((ExecutionEngineJNI)(this.ee)).DEFAULT_EXTRACT_LIMIT_BYTES = DEFAULT_LIMIT;

        for (int i=0; i< scales.length; i++) {
        	for (int j = 0; j < scales.length; j++) {
	        	ArrayList<Integer> keys = new ArrayList<Integer>();
	            keys.add(scales[i]); 
	            keys.add(scales[j]); 
	        	
	        	long tuples = NUM_TUPLES * scales[i] * scales[j];
	            LOG.info(String.format("Loading %s tuples for customers with W_ID:%s and D_ID:%s", tuples,scales[i],scales[j]));
	            this.loadTPCCData(tuples, this.customer_tbl,this.cust_p_index,keys);
        	}
        }
        LOG.info("load done");
       
    	assertTrue(true);
    	
    	VoltTable extractTable;
        long start, extract, load;
    	Pair<VoltTable,Boolean> resTable;
    	Pair<VoltTable,Boolean> resTableVerify;
        
    	
    	for (int i=0; i< scales.length; i++) {
    		for (int j=0; j< scales.length; j++) {
	    	    int warehouse_scale = scales[i];
	    	    int district_scale = scales[j];
	    	    LOG.info("Testing for warehouse scale : " + warehouse_scale + ", district scale : " + district_scale);
	    	    //extract
	    	    ReconfigurationRange<Long> districtRange = new ReconfigurationRange<Long>(this.customer_tbl.getName(), this.customer_tbl.getColumns().get(this.cust_p_index.get(1)).getName(),
	        			VoltType.SMALLINT, new Long(district_scale), new Long(district_scale+1), 1, 2, null);
	    	    ReconfigurationRange<Long> range = new ReconfigurationRange<Long>(this.customer_tbl.getName(), this.customer_tbl.getColumns().get(this.cust_p_index.get(0)).getName(),
	            		VoltType.SMALLINT, new Long(warehouse_scale), new Long(warehouse_scale+1), 1, 2, districtRange);
	            extractTable = ReconfigurationUtil.getExtractVoltTable(range);   
	            start = System.currentTimeMillis();
	            resTable= this.ee.extractTable(this.customer_tbl, this.customer_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
	            extract = System.currentTimeMillis()-start; 
	            assertFalse(resTable.getSecond());
	            LOG.info("Tuples : " + resTable.getFirst().getRowCount());
	            assertTrue(resTable.getFirst().getRowCount()==NUM_TUPLES * warehouse_scale * district_scale);
	            
	            //assert empty     
	            resTableVerify= this.ee.extractTable(this.customer_tbl, this.customer_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
	            assertTrue(resTableVerify.getFirst().getRowCount()==0);
	            
	            
	            //load
	            start = System.currentTimeMillis();
	            this.executor.loadTable(2000L, this.customer_tbl, resTable.getFirst(), false);
	            load = System.currentTimeMillis() - start;
	            LOG.info(String.format("size=%s Extract=%s Load=%s Diff:%s", NUM_TUPLES * warehouse_scale * district_scale, extract, load, load-extract));
	
	            //re extract and check its there
	            resTableVerify= this.ee.extractTable(this.customer_tbl, this.customer_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
	            assertTrue(resTableVerify.getFirst().getRowCount()==NUM_TUPLES * warehouse_scale * district_scale);
    		}
    	}

    }
    
    
}
