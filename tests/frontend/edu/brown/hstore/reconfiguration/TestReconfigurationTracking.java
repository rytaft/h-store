package edu.brown.hstore.reconfiguration;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.ReconfigurationException;
import org.voltdb.exceptions.ReconfigurationException.ExceptionTypes;

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
            + "              },"
            + "              \"table2\":{" 
            + "                \"partitions\":{"
            + "                  3 : \"1-10\"" 
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
            + "              },"
            + "              \"table2\":{" 
            + "                \"partitions\":{"
            + "                  4 : \"1-10\"" 
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
    
    @SuppressWarnings("unchecked")
    @Test
    public void testTrackReconfigurationRange() throws Exception{
        PlannedPartitions p = new PlannedPartitions(catalogContext, json_path);
        p.setPartitionPhase("1");    
        ReconfigurationPlan plan = p.setPartitionPhase("2");
        //PE 1
        ReconfigurationTrackingInterface tracking1 = new ReconfigurationTracking(p,plan,1);
        //PE 2
        ReconfigurationTrackingInterface tracking2 = new ReconfigurationTracking(p, plan,2);
        
        ReconfigurationRange<Long> migrRange = new ReconfigurationRange<Long>(tbl, VoltType.BIGINT, 100L, 110L, 2, 1);
        
        //Test single range
        tracking1.markRangeAsReceived(migrRange);
        boolean migrOut = false;
        try{
            tracking2.markRangeAsMigratedOut(migrRange);
        } catch(ReconfigurationException re){
            if (re.exceptionType == ExceptionTypes.ALL_RANGES_MIGRATED_OUT)
                migrOut = true;
        }
        assertTrue(migrOut);
        
        //check keys that should have been migrated
        assertTrue(tracking1.checkKeyOwned(tbl, 100L));
        assertTrue(tracking1.checkKeyOwned(tbl, 108L));
        
        //a key that has not been migrated
        ReconfigurationException ex = null;
        try{
            tracking1.checkKeyOwned(tbl, 110L);
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
        assertTrue(tracking2.checkKeyOwned(tbl, 110L));
        
        
        //verify moved away from 2 
        ex = null;
        try{
            tracking2.checkKeyOwned(tbl, 104L);
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
            tracking1.checkKeyOwned(tbl, 371L);
        } catch(ReconfigurationException e){
          ex =e;
        }
        assertEquals(ExceptionTypes.TUPLES_NOT_MIGRATED, ex.exceptionType);         
        range = (ReconfigurationRange<Long>) ex.dataNotYetMigrated.toArray()[0];
        assertTrue(range.getMin_inclusive() ==  371L && range.getMax_exclusive() == 371L); 
                
        //Testing an existing range split
        migrRange = new ReconfigurationRange<Long>(tbl, VoltType.BIGINT, 370L, 373L, 3, 1);
        tracking1.markRangeAsReceived(migrRange);
        assertTrue(tracking1.checkKeyOwned(tbl, 370L));        
        assertTrue(tracking1.checkKeyOwned(tbl, 371L));
        assertTrue(tracking1.checkKeyOwned(tbl, 372L));
        
        migrRange = new ReconfigurationRange<Long>(tbl, VoltType.BIGINT, 350L, 360L, 3, 1);
        tracking1.markRangeAsReceived(migrRange);
        
        //Verify keys outside of split haven't made it
        //verify moved away from 2 
        ex = null;
        try{
            tracking1.checkKeyOwned(tbl, 365L);
        } catch(ReconfigurationException e){
          ex =e;
        }
        assertEquals(ExceptionTypes.TUPLES_NOT_MIGRATED, ex.exceptionType);         
        range = (ReconfigurationRange<Long>) ex.dataNotYetMigrated.toArray()[0];
        assertTrue(range.getMin_inclusive() ==  365L && range.getMax_exclusive() == 365L);  
        
        ex = null;
        try{
            tracking1.checkKeyOwned(tbl, 369L);
        } catch(ReconfigurationException e){
          ex =e;
        }
        assertEquals(ExceptionTypes.TUPLES_NOT_MIGRATED, ex.exceptionType);         
        range = (ReconfigurationRange<Long>) ex.dataNotYetMigrated.toArray()[0];
        assertTrue(range.getMin_inclusive() ==  369L && range.getMax_exclusive() == 369L);  
        
        
        assertTrue(tracking1.checkKeyOwned(tbl, 371L));
        assertTrue(tracking1.checkKeyOwned(tbl, 355L));
        
        ex = null;
        try{
            tracking1.checkKeyOwned(tbl, 390L);
        } catch(ReconfigurationException e){
          ex =e;
        }
        assertEquals(ExceptionTypes.TUPLES_NOT_MIGRATED, ex.exceptionType);         
        range = (ReconfigurationRange<Long>) ex.dataNotYetMigrated.toArray()[0];
        assertTrue(range.getMin_inclusive() ==  390L && range.getMax_exclusive() == 390L);
        assertEquals(3, range.old_partition);
        
        
        //Test multiple ranges
        List<ReconfigurationRange<? extends Comparable<?>>> ranges = new ArrayList<>();
        ranges.add(new ReconfigurationRange<Long>(tbl, VoltType.BIGINT, 304L, 330L, 4, 1));
        ranges.add(new ReconfigurationRange<Long>(tbl, VoltType.BIGINT, 340L, 350L, 4, 1));
        ranges.add(new ReconfigurationRange<Long>(tbl, VoltType.BIGINT, 330L, 340L, 4, 1));
        tracking1.markRangeAsReceived(ranges);

        assertTrue(tracking1.checkKeyOwned(tbl, 308L));
        assertTrue(tracking1.checkKeyOwned(tbl, 334L));
        assertTrue(tracking1.checkKeyOwned(tbl, 349L));

    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testTrackReconfigurationKey() throws Exception{
        PlannedPartitions p = new PlannedPartitions(catalogContext, json_path);
        
        p.setPartitionPhase("1");    
        ReconfigurationPlan plan = p.setPartitionPhase("2");
        
        //PE 1
        ReconfigurationTrackingInterface tracking1 = new ReconfigurationTracking(p,plan,1);
        System.out.println(((ReconfigurationTracking)tracking1).PULL_SINGLE_KEY);
        //PE 2
        ReconfigurationTrackingInterface tracking2 = new ReconfigurationTracking(p, plan,2);
        ReconfigurationTrackingInterface tracking3 = new ReconfigurationTracking(p, plan,3);
        ReconfigurationTrackingInterface tracking4 = new ReconfigurationTracking(p, plan,4);
        
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
        ReconfigurationRange<Long> range = (ReconfigurationRange<Long>) ex.dataNotYetMigrated.toArray()[0];
        System.out.println(range);
        assertTrue(range.getMin_inclusive() ==  100L); 
        assertTrue(range.getMax_exclusive() == 100L); 

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
        range = (ReconfigurationRange<Long>) ex.dataMigratedOut.toArray()[0];
        assertTrue(range.getMin_inclusive() ==  100L && range.getMax_exclusive() == 100L); 
        

        //check table 2
        Set<ReconfigurationRange<? extends Comparable<?>>> pulls = new HashSet<>();
        assertTrue(tracking3.checkKeyOwned("table2", 1L));
        ex = null;
        try{
            tracking4.checkKeyOwned("table2", 1L);
        } catch(ReconfigurationException e){
          ex =e;  
        }
        assertNotNull(ex);
        assertEquals(ReconfigurationException.ExceptionTypes.TUPLES_NOT_MIGRATED,ex.exceptionType);        
        assertTrue(ex.dataNotYetMigrated.size()== 1);
        range = (ReconfigurationRange<Long>) ex.dataNotYetMigrated.toArray()[0];
        assertTrue(range.getMin_inclusive() ==  1L && range.getMax_exclusive() == 1L);
        pulls.add(range);
        pulls.add(range);
        assertEquals(pulls.size(),1);
        
        ex = null;
        try{
            tracking4.checkKeyOwned("table2", 1L);
        } catch(ReconfigurationException e){
          ex =e;  
        }
        range = (ReconfigurationRange<Long>) ex.dataNotYetMigrated.toArray()[0];
        pulls.add(range);
        
        assertEquals(pulls.size(),1);
        
        
        
        tracking3.markKeyAsMigratedOut("table2", 1L);
        tracking4.markKeyAsReceived("table2", 1L);
        assertTrue(tracking4.checkKeyOwned("table2", 1L));
        
        ex = null;
        try{
            assertTrue(tracking3.checkKeyOwned("table2", 1L));
        } catch(ReconfigurationException e){
          ex =e;  
        }
        assertNotNull(ex);
        assertEquals(ReconfigurationException.ExceptionTypes.TUPLES_MIGRATED_OUT,ex.exceptionType);        
        assertTrue(ex.dataNotYetMigrated.size()== 0);
        assertTrue(ex.dataMigratedOut.size()== 1);
        range = (ReconfigurationRange<Long>) ex.dataMigratedOut.toArray()[0];
        assertTrue(range.getMin_inclusive() ==  1L && range.getMax_exclusive() == 1L);      
        

        
        
    }

}
