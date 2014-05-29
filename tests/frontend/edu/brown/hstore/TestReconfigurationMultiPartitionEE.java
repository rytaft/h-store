/**
 * 
 */
package edu.brown.hstore;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.client.Client;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngineJNI;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTableUtil;
import org.voltdb.utils.VoltTypeUtil;

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
    private static final String ORDERS_TABLE_NAME = TPCCConstants.TABLENAME_ORDERS;
    private static final String ORDER_LINE_TABLE_NAME = TPCCConstants.TABLENAME_ORDER_LINE;
    private static final int DEFAULT_LIMIT = ExecutionEngineJNI.DEFAULT_EXTRACT_LIMIT_BYTES;
    
    private HStoreSite hstore_site;
    private HStoreConf hstore_conf;
    private Client client;

    private PartitionExecutor executor;
    private ExecutionEngine ee;
    //private Table catalog_tbl;
    private Table customer_tbl;
    private Table orders_tbl;
    private Table orderline_tbl;
    private int[] orders_p_index;
    private int[] cust_p_index;
    private int[] orderline_p_index;
    private int undo=1;
    private static boolean init = false;

    // private int ycsbTableId(Catalog catalog) {
    // return
    // catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get(TARGET_TABLE).getRelativeIndex();
    // }
    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        initializeCatalog(1, 1, NUM_PARTITIONS);
        
        // This stuff only gets initialized once
        if (!init) {
	    	JSONObject json = new JSONObject(partitionPlan);
	    	
	    	PartitionPlan pplan = new PartitionPlan();
	        pplan.fromJSON(json, this.catalog_db);
	
	        // Apply!
	        boolean secondaryIndexes = false;
	        LOG.info(String.format("Applying PartitionPlan to catalog [enableSecondaryIndexes=%s]", secondaryIndexes));
	        pplan.apply(this.catalog_db, secondaryIndexes);
	        init = true;
        }
        
        // Just make sure that the Table has the evictable flag set to true
        this.customer_tbl = getTable(CUSTOMER_TABLE_NAME);
        this.cust_p_index = new int[this.customer_tbl.getPartitioncolumns().size()];
        for(ColumnRef colRef : this.customer_tbl.getPartitioncolumns().values()) {
        	this.cust_p_index[colRef.getIndex()] = colRef.getColumn().getIndex();
    	}
        
        this.orders_tbl = getTable(ORDERS_TABLE_NAME);
        this.orders_p_index = new int[this.orders_tbl.getPartitioncolumns().size()];
        for(ColumnRef colRef : this.orders_tbl.getPartitioncolumns().values()) {
        	this.orders_p_index[colRef.getIndex()] = colRef.getColumn().getIndex();
    	}
        
        this.orderline_tbl = getTable(ORDER_LINE_TABLE_NAME);
        this.orderline_p_index = new int[this.orderline_tbl.getPartitioncolumns().size()];
        for(ColumnRef colRef : this.orderline_tbl.getPartitioncolumns().values()) {
        	this.orderline_p_index[colRef.getIndex()] = colRef.getColumn().getIndex();
    	}
        
        Site catalog_site = CollectionUtil.first(CatalogUtil.getCluster(catalog).getSites());
        hstore_conf = HStoreConf.singleton();
        
        hstore_conf.site.coordinator_sync_time = false;
        hstore_conf.global.reconfiguration_enable = true;
        //hstore_conf.global.hasher_class = "edu.brown.hashing.PlannedHasher";
        //hstore_conf.global.hasher_plan = PlannedHasher.TPCC_TEST;

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

    private void loadTPCCData(Long numTuples, Table table, int[] keyIndexes, int[] keys ) throws Exception {
    	// Load in a bunch of dummy data for this table
        VoltTable vt = CatalogUtil.getVoltTable(table);
        assertNotNull(vt);
        for (int i = 0; i < numTuples; i++) {
            Object row[] = VoltTableUtil.getRandomRow(table);
            row[0] = i;
            for(int j = 0; j < keyIndexes.length && j < keys.length; j++) {
            	row[keyIndexes[j]] = keys[j];
            }
            vt.addRow(row);
        } // FOR
        this.executor.loadTable(1000L, table, vt, false);

    }
    
    private ReconfigurationRange<Long> getReconfigurationRange(Table table, Long[][] mins, 
    		Long[][] maxs, int old_partition, int new_partition) {
    	Column[] cols = new Column[table.getPartitioncolumns().size()];
        for(ColumnRef colRef : table.getPartitioncolumns()) {
        	cols[colRef.getIndex()] = colRef.getColumn();
        }
        ArrayList<Column> colsList = new ArrayList<Column>();
        for(Column col : cols) {
        	colsList.add(col);
        }
        
        VoltTable clone = CatalogUtil.getVoltTable(colsList);

        ArrayList<Object[]> min_rows = new ArrayList<Object[]>();
        ArrayList<Object[]> max_rows = new ArrayList<Object[]>();
        int non_null_cols = 0;
        for(int i = 0; i < mins.length && i < maxs.length; i++) {
        	Long[] minsSubKeys = mins[i];
        	Long[] maxsSubKeys = maxs[i];
        	Object[] min_row = new Object[clone.getColumnCount()];
    		Object[] max_row = new Object[clone.getColumnCount()];
    		int col = 0;
    		for( ; col < minsSubKeys.length && col < maxsSubKeys.length && col < clone.getColumnCount(); col++) {
        		min_row[col] = minsSubKeys[col];
        		max_row[col] = maxsSubKeys[col];
        	}
    		non_null_cols = Math.max(non_null_cols, col);
    		for ( ; col < clone.getColumnCount(); col++) {
            	VoltType vt = clone.getColumnType(col);
            	Object obj = vt.getNullValue();
            	min_row[col] = obj;
            	max_row[col] = obj;
            }
    		min_rows.add(min_row);
    		max_rows.add(max_row);
        }
        
        VoltTable min_incl = clone.clone(0);
        VoltTable max_excl = clone.clone(0);
        for(Object[] row : min_rows) {
        	min_incl.addRow(row);
        }
        for(Object[] row : max_rows) {
        	max_excl.addRow(row);
        }

        return new ReconfigurationRange<Long>(clone, min_incl, max_excl, non_null_cols, old_partition, new_partition);
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
"    	   \"ATTRIBUTE\": \"{'ORDER_LINE#*MultiColumn*':[{'ORDER_LINE':'OL_W_ID'},{'ORDER_LINE':'OL_D_ID'},{'ORDER_LINE':'OL_O_ID'},{'ORDER_LINE':'OL_NUMBER'}]}\"," +
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
    	int[] keys = new int[]{ wid };
        
    	ReconfigurationRange<Long> range; 
        VoltTable extractTable;
        Long[][] mins = new Long[][]{{ new Long(wid) }};
    	Long[][] maxs = new Long[][]{{ new Long(wid+1) }};
    	range = getReconfigurationRange(this.customer_tbl, mins, maxs, 1, 2);
        ArrayList<VoltType> types = new ArrayList<>();
	    for(int col : cust_p_index) {
	    	types.add(VoltType.get((byte) this.customer_tbl.getColumns().get(col).getType()));
	    }
        extractTable = ReconfigurationUtil.getExtractVoltTable(range, this.cust_p_index.length, types);   
        this.loadTPCCData(NUM_TUPLES * 10, this.customer_tbl,this.cust_p_index, keys);
        int EXTRACT_LIMIT = 2048;
        ((ExecutionEngineJNI)(this.ee)).DEFAULT_EXTRACT_LIMIT_BYTES = EXTRACT_LIMIT;
        
        long tupleBytes = MemoryEstimator.estimateTupleSize(this.customer_tbl);
        int tuplesInChunk = (int)(EXTRACT_LIMIT / tupleBytes);
        int expectedChunks = (int) Math.ceil((double)(NUM_TUPLES * 10)/tuplesInChunk);
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
        	long tuples = NUM_TUPLES * scales[i];
            LOG.info(String.format("Loading %s tuples for customers with W_ID :%s ", tuples,scales[i]));
            this.loadTPCCData(tuples, this.customer_tbl,this.cust_p_index,new int[]{ scales[i] });
                
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
    	    
    	    Long[][] mins = new Long[][]{{ new Long(scale) }};
        	Long[][] maxs = new Long[][]{{ new Long(scale+1) }};
        	range = getReconfigurationRange(this.customer_tbl, mins, maxs, 1, 2);
            ArrayList<VoltType> types = new ArrayList<>();
    	    for(int col : cust_p_index) {
    	    	types.add(VoltType.get((byte) this.customer_tbl.getColumns().get(col).getType()));
    	    }
            extractTable = ReconfigurationUtil.getExtractVoltTable(range, this.cust_p_index.length, types);   
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
    	int[] keys = new int[]{ wid, did };
        
    	Long[][] mins = new Long[][]{{ new Long(wid), new Long(did) }};
    	Long[][] maxs = new Long[][]{{ new Long(wid+1), new Long(did+1) }};
    	ReconfigurationRange<Long> range = getReconfigurationRange(this.customer_tbl, mins, maxs, 1, 2);
        ArrayList<VoltType> types = new ArrayList<>();
	    for(int col : cust_p_index) {
	    	types.add(VoltType.get((byte) this.customer_tbl.getColumns().get(col).getType()));
	    }
        VoltTable extractTable = ReconfigurationUtil.getExtractVoltTable(range, this.cust_p_index.length, types);   
        this.loadTPCCData(NUM_TUPLES * 10, this.customer_tbl,this.cust_p_index, keys);
        int EXTRACT_LIMIT = 2048;
        ((ExecutionEngineJNI)(this.ee)).DEFAULT_EXTRACT_LIMIT_BYTES = EXTRACT_LIMIT;
        
        long tupleBytes = MemoryEstimator.estimateTupleSize(this.customer_tbl);
        int tuplesInChunk = (int)(EXTRACT_LIMIT / tupleBytes);
        int expectedChunks = (int) Math.ceil((double)(NUM_TUPLES * 10)/tuplesInChunk);
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
	        	long tuples = NUM_TUPLES * scales[i] * scales[j];
	            LOG.info(String.format("Loading %s tuples for customers with W_ID:%s and D_ID:%s", tuples,scales[i],scales[j]));
	            this.loadTPCCData(tuples, this.customer_tbl,this.cust_p_index,new int[]{ scales[i], scales[j] });
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
	    	    Long[][] mins = new Long[][]{{ new Long(warehouse_scale), new Long(district_scale) }};
	        	Long[][] maxs = new Long[][]{{ new Long(warehouse_scale+1), new Long(district_scale+1) }};
	        	ReconfigurationRange<Long> range = getReconfigurationRange(this.customer_tbl, mins, maxs, 1, 2);
	            ArrayList<VoltType> types = new ArrayList<>();
	    	    for(int col : cust_p_index) {
	    	    	types.add(VoltType.get((byte) this.customer_tbl.getColumns().get(col).getType()));
	    	    }
	            extractTable = ReconfigurationUtil.getExtractVoltTable(range, this.cust_p_index.length, types);   
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
    
    @Test
    public void testSingleColumnRangeExtractAllOrders() throws Exception {  
    	int wid = 2; // warehouse id
    	int[] keys = new int[]{ wid };
        
    	ReconfigurationRange<Long> range; 
        VoltTable extractTable;
        Long[][] mins = new Long[][]{{ new Long(wid) }};
    	Long[][] maxs = new Long[][]{{ new Long(wid+1) }};
    	range = getReconfigurationRange(this.orders_tbl, mins, maxs, 1, 2);
        ArrayList<VoltType> types = new ArrayList<>();
	    for(int col : orders_p_index) {
	    	types.add(VoltType.get((byte) this.orders_tbl.getColumns().get(col).getType()));
	    }
        extractTable = ReconfigurationUtil.getExtractVoltTable(range, this.orders_p_index.length, types);   
        this.loadTPCCData(NUM_TUPLES * 10, this.orders_tbl,this.orders_p_index, keys);
        int EXTRACT_LIMIT = 2048;
        ((ExecutionEngineJNI)(this.ee)).DEFAULT_EXTRACT_LIMIT_BYTES = EXTRACT_LIMIT;
        
        long tupleBytes = MemoryEstimator.estimateTupleSize(this.orders_tbl);
        int tuplesInChunk = (int)(EXTRACT_LIMIT / tupleBytes);
        int expectedChunks = (int) Math.ceil((double)(NUM_TUPLES * 10)/tuplesInChunk);
        int resCount = 0;
        int chunks = 0;
        Pair<VoltTable,Boolean> resTable = 
                this.ee.extractTable(this.orders_tbl, this.orders_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
        assertTrue(resTable.getSecond());
        resCount += resTable.getFirst().getRowCount();
        chunks++;
        while(resTable.getSecond()){
            resTable = 
                    this.ee.extractTable(this.orders_tbl, this.orders_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
            resCount += resTable.getFirst().getRowCount();
            chunks++;
        }
        assertEquals(expectedChunks, chunks);
        assertEquals(NUM_TUPLES*10, resCount);
    }
    
    @Test
    public void testSingleColumnRangeOrders() throws Exception {  
    	((ExecutionEngineJNI)(this.ee)).DEFAULT_EXTRACT_LIMIT_BYTES = DEFAULT_LIMIT;

        for (int i=0; i< scales.length; i++) {
        	long tuples = NUM_TUPLES * scales[i];
            LOG.info(String.format("Loading %s tuples for orders with W_ID :%s ", tuples,scales[i]));
            this.loadTPCCData(tuples, this.orders_tbl,this.orders_p_index,new int[]{ scales[i] });
                
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
    	    Long[][] mins = new Long[][]{{ new Long(scale) }};
        	Long[][] maxs = new Long[][]{{ new Long(scale+1) }};
        	range = getReconfigurationRange(this.orders_tbl, mins, maxs, 1, 2);
            ArrayList<VoltType> types = new ArrayList<>();
    	    for(int col : orders_p_index) {
    	    	types.add(VoltType.get((byte) this.orders_tbl.getColumns().get(col).getType()));
    	    }
            extractTable = ReconfigurationUtil.getExtractVoltTable(range, this.orders_p_index.length, types);   
            start = System.currentTimeMillis();
            resTable= this.ee.extractTable(this.orders_tbl, this.orders_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
            extract = System.currentTimeMillis()-start; 
            assertFalse(resTable.getSecond());
            LOG.info("Tuples : " + resTable.getFirst().getRowCount());
            assertTrue(resTable.getFirst().getRowCount()==NUM_TUPLES *scale);
            
            //assert empty     
            resTableVerify= this.ee.extractTable(this.orders_tbl, this.orders_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
            assertTrue(resTableVerify.getFirst().getRowCount()==0);
            
            
            //load
            start = System.currentTimeMillis();
            this.executor.loadTable(2000L, this.orders_tbl, resTable.getFirst(), false);
            load = System.currentTimeMillis() - start;
            LOG.info(String.format("size=%s Extract=%s Load=%s Diff:%s", NUM_TUPLES*scale, extract, load, load-extract));

            //re extract and check its there
            resTableVerify= this.ee.extractTable(this.orders_tbl, this.orders_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
            assertTrue(resTableVerify.getFirst().getRowCount()==NUM_TUPLES *scale);
    	}

    }
    
    @Test
    public void testTwoColumnRangeExtractAllOrders() throws Exception {
    	int wid = 2; // warehouse id
    	int did = 3; // district id
    	int[] keys = new int[]{ wid, did };
        
    	Long[][] mins = new Long[][]{{ new Long(wid), new Long(did) }};
    	Long[][] maxs = new Long[][]{{ new Long(wid+1), new Long(did+1) }};
    	ReconfigurationRange<Long> range = getReconfigurationRange(this.orders_tbl, mins, maxs, 1, 2);
        ArrayList<VoltType> types = new ArrayList<>();
	for(int col : orders_p_index) {
	    types.add(VoltType.get((byte) this.orders_tbl.getColumns().get(col).getType()));
	}
	VoltTable extractTable = ReconfigurationUtil.getExtractVoltTable(range, this.orders_p_index.length, types);  
        this.loadTPCCData(NUM_TUPLES * 10, this.orders_tbl,this.orders_p_index, keys);
        int EXTRACT_LIMIT = 2048;
        ((ExecutionEngineJNI)(this.ee)).DEFAULT_EXTRACT_LIMIT_BYTES = EXTRACT_LIMIT;
        
        long tupleBytes = MemoryEstimator.estimateTupleSize(this.orders_tbl);
        int tuplesInChunk = (int)(EXTRACT_LIMIT / tupleBytes);
        int expectedChunks = (int) Math.ceil((double)(NUM_TUPLES * 10)/tuplesInChunk);
        int resCount = 0;
        int chunks = 0;
        Pair<VoltTable,Boolean> resTable = 
                this.ee.extractTable(this.orders_tbl, this.orders_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
        assertTrue(resTable.getSecond());
        resCount += resTable.getFirst().getRowCount();
        chunks++;
        while(resTable.getSecond()){
            resTable = 
                    this.ee.extractTable(this.orders_tbl, this.orders_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
            resCount += resTable.getFirst().getRowCount();
            chunks++;
        }
        assertEquals(expectedChunks, chunks);
        assertEquals(NUM_TUPLES*10, resCount);
    }
    
    @Test
    public void testTwoColumnRangeOrders() throws Exception {
    	((ExecutionEngineJNI)(this.ee)).DEFAULT_EXTRACT_LIMIT_BYTES = DEFAULT_LIMIT;

        for (int i=0; i< scales.length; i++) {
        	for (int j = 0; j < scales.length; j++) {
	        	long tuples = NUM_TUPLES * scales[i] * scales[j];
	            LOG.info(String.format("Loading %s tuples for orders with W_ID:%s and D_ID:%s", tuples,scales[i],scales[j]));
	            this.loadTPCCData(tuples, this.orders_tbl,this.orders_p_index,new int[]{ scales[i], scales[j] });
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
	    	    Long[][] mins = new Long[][]{{ new Long(warehouse_scale), new Long(district_scale) }};
	        	Long[][] maxs = new Long[][]{{ new Long(warehouse_scale+1), new Long(district_scale+1) }};
	        	ReconfigurationRange<Long> range = getReconfigurationRange(this.orders_tbl, mins, maxs, 1, 2);
	            ArrayList<VoltType> types = new ArrayList<>();
	    	    for(int col : orders_p_index) {
	    	    	types.add(VoltType.get((byte) this.orders_tbl.getColumns().get(col).getType()));
	    	    }
	            extractTable = ReconfigurationUtil.getExtractVoltTable(range, this.orders_p_index.length, types);   
	            start = System.currentTimeMillis();
	            resTable= this.ee.extractTable(this.orders_tbl, this.orders_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
	            extract = System.currentTimeMillis()-start; 
	            assertFalse(resTable.getSecond());
	            LOG.info("Tuples : " + resTable.getFirst().getRowCount());
	            assertTrue(resTable.getFirst().getRowCount()==NUM_TUPLES * warehouse_scale * district_scale);
	            
	            //assert empty     
	            resTableVerify= this.ee.extractTable(this.orders_tbl, this.orders_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
	            assertTrue(resTableVerify.getFirst().getRowCount()==0);
	            
	            
	            //load
	            start = System.currentTimeMillis();
	            this.executor.loadTable(2000L, this.orders_tbl, resTable.getFirst(), false);
	            load = System.currentTimeMillis() - start;
	            LOG.info(String.format("size=%s Extract=%s Load=%s Diff:%s", NUM_TUPLES * warehouse_scale * district_scale, extract, load, load-extract));
	
	            //re extract and check its there
	            resTableVerify= this.ee.extractTable(this.orders_tbl, this.orders_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
	            assertTrue(resTableVerify.getFirst().getRowCount()==NUM_TUPLES * warehouse_scale * district_scale);
    		}
    	}

    }

    public void testBTreeSearch() throws Exception {
    	((ExecutionEngineJNI)(this.ee)).DEFAULT_EXTRACT_LIMIT_BYTES = DEFAULT_LIMIT;

        for (int i=0; i< scales.length; i++) {
	    for (int j = 0; j < scales.length; j++) {
		for (int k = 0; k < scales.length; k++) {
		    LOG.info(String.format("Loading %s tuples for order lines with W_ID:%s, D_ID:%s and O_ID:%s", NUM_TUPLES,scales[i],scales[j],scales[k]));
		    for (int l = 0; l < NUM_TUPLES; l++) {
			this.loadTPCCData(new Long(1), this.orderline_tbl,this.orderline_p_index,new int[]{ scales[i], scales[j], scales[k], l });
		    }
		}
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
    		for (int k=0; k< scales.length; k++) {
	    	    int warehouse_scale = scales[i];
	    	    int district_scale = scales[j];
	    	    int order_scale = scales[k];
	    	    LOG.info("Testing for warehouse scale : " + warehouse_scale + ", district scale : " + district_scale + ", order scale : " + order_scale);
	    	    //extract
		    
	    	    Long[][] mins = new Long[][]{
	    	    		{ new Long(warehouse_scale), new Long(district_scale), new Long(order_scale), new Long(2) },
		    			{ new Long(warehouse_scale), new Long(district_scale), new Long(order_scale), new Long(37) },
		    			{ new Long(warehouse_scale), new Long(district_scale), new Long(order_scale), new Long(90) }};
	    	    Long[][] maxs = new Long[][]{
	    	    		{ new Long(warehouse_scale+1), new Long(district_scale+1), new Long(order_scale+1), new Long(20) },
        				{ new Long(warehouse_scale+1), new Long(district_scale+1), new Long(order_scale+1), new Long(38) },
        				{ new Long(warehouse_scale+1), new Long(district_scale+1), new Long(order_scale+1), new Long(95) }};
	    	    ReconfigurationRange<Long> range = getReconfigurationRange(this.orderline_tbl, mins, maxs, 1, 2);
        
	    	    ArrayList<VoltType> types = new ArrayList<>();
	    	    for(int col : orderline_p_index) {
	    	    	types.add(VoltType.get((byte) this.orderline_tbl.getColumns().get(col).getType()));
	    	    }
	            extractTable = ReconfigurationUtil.getExtractVoltTable(range, this.orderline_p_index.length, types);   
	            start = System.currentTimeMillis();
	            resTable= this.ee.extractTable(this.orderline_tbl, this.orderline_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
	            extract = System.currentTimeMillis()-start; 
	            assertFalse(resTable.getSecond());
	            LOG.info("Tuples : " + resTable.getFirst().getRowCount());
	            assertTrue(resTable.getFirst().getRowCount()== 24);
	            
	            //assert empty     
	            resTableVerify= this.ee.extractTable(this.orderline_tbl, this.orderline_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
	            assertTrue(resTableVerify.getFirst().getRowCount()==0);
	            
	            
	            //load
	            start = System.currentTimeMillis();
	            this.executor.loadTable(2000L, this.orderline_tbl, resTable.getFirst(), false);
	            load = System.currentTimeMillis() - start;
	            LOG.info(String.format("size=%s Extract=%s Load=%s Diff:%s", 24, extract, load, load-extract));
	
	            //re extract and check its there
	            resTableVerify= this.ee.extractTable(this.orderline_tbl, this.orderline_tbl.getRelativeIndex(), extractTable, 1, 1, undo++, -1, 1);
	            assertTrue(resTableVerify.getFirst().getRowCount()==24);
    		}
	    }
	}
    }    
}
