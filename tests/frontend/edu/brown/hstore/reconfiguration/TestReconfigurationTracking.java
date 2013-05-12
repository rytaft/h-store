package edu.brown.hstore.reconfiguration;

import java.io.File;

import org.json.JSONObject;
import org.junit.Test;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.ReconfigurationException;

import edu.brown.BaseTestCase;
import edu.brown.hashing.PlannedPartitions;
import edu.brown.hashing.ReconfigurationPlan;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;

public class TestReconfigurationTracking extends BaseTestCase {
    public String test_json1 = "{" 
            + "       \"default_table\":\"usertable\"," 
            + "       " +
    		"\"partition_plans\":{" 
            + "          \"1\" : {"
            + "            \"tables\":{" 
            + "              \"usertable\":{" 
            + "                \"partitions\":{"
            + "                  1 : \"1-100\"," 
            + "                  2 : \"100-300\"," 
            + "                  3 : \"300,350-400,302\","
            + "                  4 : \"301,303,304-350\"       " 
            + "                }     " 
            + "              }" 
            + "            }"
            + "          }," 
            + "          \"2\" : {"
            + "            \"tables\":{" 
            + "              \"usertable\":{"
            + "                \"partitions\":{" 
            + "                  1 : \"1-400\"," 
            + "                }     " 
            + "              }"
            + "            }" 
            + "          }" 
            + "        }" 
            + "}";
    private File json_path;
    String tbl = "usertable"; 
    @Override
    protected void setUp() throws Exception {
      super.setUp(ProjectType.YCSB);
      Table catalog_tbl = this.getTable(tbl);
      Column catalog_col = this.getColumn(catalog_tbl, "YCSB_KEY");
      catalog_tbl.setPartitioncolumn(catalog_col);
      String tmp_dir = System.getProperty("java.io.tmpdir");
      json_path = FileUtil.join(tmp_dir, "test1.json");
      FileUtil.writeStringToFile(json_path, test_json1);
      
    }
    
    public TestReconfigurationTracking() {
        super();
    }
    
    @Test
    public void testTrack() throws Exception{
        PlannedPartitions p = new PlannedPartitions(catalogContext, json_path);
        p.setPartitionPhase("1");    
        ReconfigurationPlan plan = p.setPartitionPhase("2");
        //PE 1
        ReconfigurationTrackingInterface tracking1 = new ReconfigurationTracking(p,plan,1);
        //PE 2
        ReconfigurationTrackingInterface tracking2 = new ReconfigurationTracking(p, plan,2);
        
        //keys that should be present
        assertTrue(tracking1.checkKeyOwned(tbl, 1L));
        assertTrue(tracking1.checkKeyOwned(tbl, 99L));
        
        //Check a  key that is not migrated yet, but should be

        assertTrue(tracking2.checkKeyOwned(tbl, 100L));
        ReconfigurationException ex = null;
        try{
            tracking1.checkKeyOwned(tbl, 100L);
        } catch(ReconfigurationException e){
          ex =e;  
        } catch(Exception e){
          e.printStackTrace();   
        }
        assertNotNull(ex);
        assertEquals(ReconfigurationException.ExceptionTypes.TUPLES_NOT_MIGRATED,ex.exceptionType);        
        assertTrue(ex.dataNotYetMigrated.size()== 1);
        ReconfigurationRange<Long> range = (ReconfigurationRange<Long>) ex.dataNotYetMigrated.get(0);
        assertTrue(range.getMin_inclusive() ==  100L && range.getMax_exclusive() == 100L); 
        
        //'migrate key'
        tracking1.markKeyAsReceived(tbl, 100L);
        tracking2.markKeyAsMigratedOut(tbl, 100L);
        
        //verify moved
        assertTrue(tracking1.checkKeyOwned(tbl, 100L));
        
        //verify moved away from 2 
        ex = null;
        try{
            tracking2.checkKeyOwned(tbl, 100L);
        } catch(ReconfigurationException e){
          ex =e;  
        }

        assertNotNull(ex);
        assertEquals(ReconfigurationException.ExceptionTypes.TUPLES_MIGRATED_OUT,ex.exceptionType);        
        assertTrue(ex.dataNotYetMigrated.size()== 0);
        assertTrue(ex.dataMigratedOut.size()== 1);
        range = (ReconfigurationRange<Long>) ex.dataMigratedOut.get(0);
        assertTrue(range.getMin_inclusive() ==  100L && range.getMax_exclusive() == 100L); 
        
        //TODO check range
        //TODO migrate range, check middle
        //TODO migrate have exception range have both
        
    }

}
