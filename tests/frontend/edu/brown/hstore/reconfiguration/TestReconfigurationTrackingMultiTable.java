package edu.brown.hstore.reconfiguration;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.ReconfigurationException;
import org.voltdb.exceptions.ReconfigurationException.ExceptionTypes;

import edu.brown.BaseTestCase;
import edu.brown.hashing.ExplicitPartitions;
import edu.brown.hashing.PlannedPartitions;
import edu.brown.hashing.ReconfigurationPlan;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.hashing.TwoTieredRangePartitions;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;

public class TestReconfigurationTrackingMultiTable extends BaseTestCase {
    public String test_json1 = "{" 
            + "       \"default_table\":\"warehouse\"," 
            + "       " +
    		"\"partition_plan\":{" 
            + "            \"tables\":{" 
            + "              \"warehouse\":{" 
            + "                \"partitions\":{"
            + "                  1 : \"0-1\"," 
            + "                  2 : \"1-4\","
            + "                }" 
            + "              }"            
            + "            }"
            + "        }" 
            + "}";

    public String test_json2 = "{" 
            + "       \"default_table\":\"warehouse\"," 
            + "       " +
    		"\"partition_plan\":{" 
            + "            \"tables\":{" 
            + "              \"warehouse\":{"
            + "                \"partitions\":{" 
            + "                  1 : \"0-4\"," 
            + "                }"    
            + "              }"
            + "            }" 
            + "        }" 
            + "}";

    private File json_path1;
    private File json_path2;
    
    private int warehouseTableId(Catalog catalog) {
        return catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get("WAREHOUSE").getRelativeIndex();
    }

    private int stockTableId(Catalog catalog) {
        return catalog.getClusters().get("cluster").getDatabases().get("database").getTables().get("STOCK").getRelativeIndex();
    }
    
    String customer = "customer"; 
    String warehouse = "warehouse";
    private Object cust_p_index;
    private Table customer_tbl;
    private Table warehouse_tbl;
    private int warehouse_p_index; 
    @Override
    protected void setUp() throws Exception {
      super.setUp(ProjectType.TPCC);
      this.customer_tbl = this.getTable(customer);
      this.cust_p_index = this.customer_tbl.getPartitioncolumn().getIndex();
      this.warehouse_tbl = this.getTable(warehouse);
      this.warehouse_p_index = this.warehouse_tbl.getPartitioncolumn().getIndex();
      

      String tmp_dir = System.getProperty("java.io.tmpdir");
      json_path1 = FileUtil.join(tmp_dir, "test1.json");
      FileUtil.writeStringToFile(json_path1, test_json1);
      json_path2 = FileUtil.join(tmp_dir, "test2.json");
      FileUtil.writeStringToFile(json_path2, test_json2);
    }
    
    public TestReconfigurationTrackingMultiTable() {
        super();
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testTrackReconfigurationRange() throws Exception{
        ExplicitPartitions p = new TwoTieredRangePartitions(catalogContext, json_path1);
        p.setPartitionPlan(json_path1);
	ReconfigurationPlan plan = p.setPartitionPlan(json_path2);
        //PE 1
        ReconfigurationTrackingInterface tracking1 = new ReconfigurationTracking(p,plan,1);
        //PE 2
        ReconfigurationTrackingInterface tracking2 = new ReconfigurationTracking(p, plan,2);
        
        
        
        //a key that has not been migrated
        ReconfigurationException ex = null;
        try{
            tracking1.checkKeyOwned(customer_tbl, Arrays.asList(new Object[]{2L}));
        } catch(ReconfigurationException e){
          ex =e;  
        } catch(Exception e){
          e.printStackTrace();   
        }
        assertNotNull(ex);
        assertEquals(ReconfigurationException.ExceptionTypes.TUPLES_NOT_MIGRATED,ex.exceptionType);        
        assertEquals(3,ex.dataNotYetMigrated.size());
        for (ReconfigurationRange range : ex.dataNotYetMigrated) {
            System.out.println(range);
            tracking1.markKeyAsReceived(range.getTableName(), Arrays.asList(range.getMinIncl().getRowArray()));
            tracking2.markKeyAsMigratedOut(range.getTableName(), Arrays.asList(range.getMinIncl().getRowArray()));
        }
        
        assertTrue(tracking1.checkKeyOwned(warehouse_tbl.getPartitioncolumn(), new Short("2")));
        
        
        
        ReconfigurationRange migrRange = ReconfigurationUtil.getReconfigurationRange(customer_tbl, new Long[]{1L}, new Long[]{2L}, 2, 1);
        //Test single range
        tracking1.markRangeAsReceived(migrRange);
        tracking2.markRangeAsMigratedOut(migrRange);

        
        //check keys that should have been migrated
        assertTrue(tracking1.checkKeyOwned(customer_tbl, Arrays.asList(new Object[]{1L})));
        assertTrue(tracking1.checkKeyOwned(customer_tbl, Arrays.asList(new Object[]{0L})));
        
        
        
        boolean migrOut = false;
        try{
            tracking2.markRangeAsMigratedOut(migrRange);
        } catch(ReconfigurationException re){
            if (re.exceptionType == ExceptionTypes.ALL_RANGES_MIGRATED_OUT)
                migrOut = true;
        }
        //assertTrue(migrOut);
        /*
        //a key that has not been migrated
        ReconfigurationException ex = null;
        try{
            tracking1.checkKeyOwned(customer, 110L);
        } catch(ReconfigurationException e){
          ex =e;  
        } catch(Exception e){
          e.printStackTrace();   
        }
        assertNotNull(ex);
        assertEquals(ReconfigurationException.ExceptionTypes.TUPLES_NOT_MIGRATED,ex.exceptionType);        
        assertTrue(ex.dataNotYetMigrated.size()== 1);
        ReconfigurationRange<Long> range = (ReconfigurationRange<Long>) ex.dataNotYetMigrated.toArray()[0];
        
        assertTrue(range.getMin_inclusive() ==  110L);
        assertTrue(range.getMax_exclusive() == 110L);
        
        //source still has the key
        assertTrue(tracking2.checkKeyOwned(customer, 110L));
        
        
        //verify moved away from 2 
        ex = null;
        try{
            tracking2.checkKeyOwned(customer, 104L);
        } catch(ReconfigurationException e){
          ex =e;  
        }
        assertNotNull(ex);
        assertEquals(ReconfigurationException.ExceptionTypes.TUPLES_MIGRATED_OUT,ex.exceptionType);         
        range = (ReconfigurationRange<Long>) ex.dataMigratedOut.toArray()[0];//.get(0);
        assertTrue(range.getMin_inclusive() ==  104L && range.getMax_exclusive() == 104L); 
        
        
        //verify moved away from 2 
        ex = null;
        try{
            tracking1.checkKeyOwned(customer, 371L);
        } catch(ReconfigurationException e){
          ex =e;
        }
        assertEquals(ExceptionTypes.TUPLES_NOT_MIGRATED, ex.exceptionType);         
        range = (ReconfigurationRange<Long>) ex.dataNotYetMigrated.toArray()[0];
        assertTrue(range.getMin_inclusive() ==  371L && range.getMax_exclusive() == 371L); 
        */


    }
    


}
