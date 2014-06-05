package edu.brown.hashing;

import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.hashing.PlannedPartitions.PartitionPhase;
import edu.brown.hashing.PlannedPartitions.PartitionRange;
import edu.brown.hashing.PlannedPartitions.PartitionedTable;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationTable;
import edu.brown.hstore.TestReconfigurationMultiPartitionEE;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;

public class TestMultiColumnTwoTieredRangePartitions extends BaseTestCase {

	private static final Logger LOG = Logger.getLogger(TestMultiColumnTwoTieredRangePartitions.class);
    private static boolean init = false;
	
    public TestMultiColumnTwoTieredRangePartitions() {

    }

    public String test_json1 = "{"+
            "  \"partition_plan\": {"+
            "    \"tables\": {"+
            "      \"district\": {"+
            "        \"partitions\": {"+
            "          \"0\": \"1-1:3\", "+
            "          \"1\": \"1:3-2,2-3\""+
            "        }"+
            "      },"+
            "      \"stock\": {"+
            "        \"partitions\": {"+
            "          \"0\": \"1-2\", "+
            "          \"1\": \"2-3\""+
            "        }"+
            "      }"+
            "    }"+
            "  }, "+
            "  \"default_table\": \"warehouse\""+
            "}";
  
    public String test_json2 = "{"+
            "  \"partition_plan\": {"+
            "    \"tables\": {"+
            "      \"district\": {"+
            "        \"partitions\": {"+
            "          \"0\": \"1-2\", "+
            "          \"1\": \"2-3\""+
            "        }"+
            "      },"+
            "      \"stock\": {"+
            "        \"partitions\": {"+
            "          \"0\": \"1-2\", "+
            "          \"1\": \"2-3\""+
            "        }"+
            "      }"+
            "    }"+
            "  }, "+
            "  \"default_table\": \"warehouse\""+
            "}";

    
    private File json_path1;
    private File json_path2;
    
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
"    	   \"ATTRIBUTE\": \"{'STOCK':'S_W_ID'}\"," +
"    	   \"ATTRIBUTE_class\": \"org.voltdb.catalog.Column\"" +
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

    @Override
    protected void setUp() throws Exception {
    	super.setUp(ProjectType.TPCC);

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
        
        String tmp_dir = System.getProperty("java.io.tmpdir");
        json_path1 = FileUtil.join(tmp_dir, "test1.json");
        FileUtil.writeStringToFile(json_path1, test_json1);
        json_path2 = FileUtil.join(tmp_dir, "test2.json");
        FileUtil.writeStringToFile(json_path2, test_json2);
    }

    public void testReadJSON() throws Exception {
        File f1 = new File(json_path1.getAbsolutePath());
        assertNotNull(f1);
        assert (f1.exists());
        JSONObject test_json = new JSONObject(FileUtil.readFile(f1));
        ExplicitPartitions p = new TwoTieredRangePartitions(catalogContext, test_json);
        p.setPartitionPlan(json_path1);
        assertEquals(0, p.getPartitionId("district", new Long[]{ 1L, 1L }));
        assertEquals(1, p.getPartitionId("district", new Long[]{ 1L, 4L }));
    }

    public void testExtractTableNames() throws Exception {
        JSONObject test_json = new JSONObject(test_json1);
        ExplicitPartitions p = new TwoTieredRangePartitions(catalogContext, test_json);
        Set<String> tbls = p.getExplicitPartitionedTables(test_json);
        assertTrue(tbls.contains("district"));
    }

    public void testBuildTablePartitions() throws Exception {
        JSONObject test_json = new JSONObject(test_json1);
        ExplicitPartitions p = new TwoTieredRangePartitions(catalogContext, test_json);
        p.setPartitionPlan(json_path1);
        assertEquals(0, p.getPartitionId("district", new Long[]{ 1L, 1L }));
        assertEquals(1, p.getPartitionId("district", new Long[]{ 1L, 5L }));
        assertEquals(1, p.getPartitionId("district", new Long[]{ 2L, 2L }));
        assertEquals(1, p.getPartitionId("district", new Long[]{ 2L }));
        assertEquals(-1, p.getPartitionId("district", new Long[]{ 1L }));

        p.setPartitionPlan(json_path2);
        assertEquals(0, p.getPartitionId("district", new Long[]{ 1L, 1L }));
        assertEquals(0, p.getPartitionId("district", new Long[]{ 1L, 5L }));
        assertEquals(1, p.getPartitionId("district", new Long[]{ 2L, 2L }));
        assertEquals(1, p.getPartitionId("district", new Long[]{ 2L }));
        assertEquals(0, p.getPartitionId("district", new Long[]{ 1L }));
    }

    public void testPartitionRangeCompare() throws Exception {
        PartitionRange pr1 = new PartitionRange(getTable(TPCCConstants.TABLENAME_DISTRICT), 1, "1:1-1:20");
        PartitionRange pr2 = new PartitionRange(getTable(TPCCConstants.TABLENAME_DISTRICT), 1, "1:2-1:3");
        assertTrue(pr1.compareTo(pr2) < 0);
        assertTrue(pr2.compareTo(pr1) > 0);
    }

    @SuppressWarnings("unchecked")
    public void testReconfigurationPlan() throws Exception {
        List<PartitionRange> olds = new ArrayList<>();
        List<PartitionRange> news = new ArrayList<>();

        Table table = getTable(TPCCConstants.TABLENAME_DISTRICT);
        olds.add(new PartitionRange(table, 1, "1-2"));
        PartitionedTable old_table = new PartitionedTable(olds, "table", table);
        Map<String, PartitionedTable> old_table_map = new HashMap<String, PlannedPartitions.PartitionedTable>();
        old_table_map.put("table", old_table);
        PartitionPhase old_phase = new PartitionPhase(old_table_map);

        news.add(new PartitionRange(table, 1, "1-1:10"));
        news.add(new PartitionRange(table, 2, "1:10-1:20"));
        news.add(new PartitionRange(table, 3, "1:20-2"));
        PartitionedTable new_table = new PartitionedTable(news, "table", table);
        Map<String, PartitionedTable> new_table_map = new HashMap<String, PlannedPartitions.PartitionedTable>();
        new_table_map.put("table", new_table);
        PartitionPhase new_phase = new PartitionPhase(new_table_map);

        ReconfigurationPlan reconfig_plan = new ReconfigurationPlan(old_phase, new_phase);

        ReconfigurationTable reconfig = (ReconfigurationTable) reconfig_plan.tables_map.get("table");
        ReconfigurationRange range = null;
        range = reconfig.getReconfigurations().get(0);
        assertTrue(((Long)range.getMinIncl().get(0)[1]) == 10 && ((Long)range.getMaxExcl().get(0)[1]) == 20 && range.getOldPartition() == 1 && range.getNewPartition() == 2);

        range = reconfig.getReconfigurations().get(1);
        assertTrue(((Long)range.getMinIncl().get(0)[1]) == 20 && range.getKeySchema().getColumnType(1).getNullValue().equals(range.getMaxExcl().get(0)[1]) && range.getOldPartition() == 1 && range.getNewPartition() == 3);

    }
}
