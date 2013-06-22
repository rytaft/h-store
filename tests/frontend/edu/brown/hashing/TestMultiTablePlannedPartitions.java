package edu.brown.hashing;

import java.io.File;
import java.util.List;
import java.util.Set;

import org.json.JSONObject;

import edu.brown.BaseTestCase;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;

public class TestMultiTablePlannedPartitions extends BaseTestCase {
    private File json_path;
    public String tpcc_json = "{"+
            "  \"partition_plans\": {"+
            "    \"1\": {"+
            "      \"tables\": {"+
            "        \"warehouse\": {"+
            "          \"partitions\": {"+
            "            \"0\": \"1-2\", "+
            "            \"1\": \"2-3\""+
            "          }"+
            "        }, "+
            "        \"item\": {"+
            "          \"partitions\": {"+
            "            \"0\": \"0-50000\", "+
            "            \"1\": \"50000-100000\""+
            "          }"+
            "        }"+
            "      }"+
            "    }, "+
            "    \"2\": {"+
            "      \"tables\": {"+
            "        \"warehouse\": {"+
            "          \"partitions\": {"+
            "            \"1\": \"1-3\""+
            "          }"+
            "        }, "+
            "        \"item\": {"+
            "          \"partitions\": {"+
            "            \"1\": \"0-100000\""+
            "          }"+
            "        }"+
            "      }"+
            "    }"+
            "  }, "+
            "  \"default_table\": \"warehouse\""+
            "}";
  
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        // Table catalog_tbl = this.getTable("USERTABLE");
        // Column catalog_col = this.getColumn(catalog_tbl, "YCSB_KEY");
        // catalog_tbl.setPartitioncolumn(catalog_col);
        String tmp_dir = System.getProperty("java.io.tmpdir");
        json_path = FileUtil.join(tmp_dir, "test_tpcc.json");
        FileUtil.writeStringToFile(json_path, tpcc_json);
    }

    public void testExtractTableNames() throws Exception {
        JSONObject test_json = new JSONObject(tpcc_json);
        Set<String> tbls = PlannedPartitions.getExplicitPartitionedTables(test_json);
        assertTrue(tbls.contains("warehouse"));
        assertFalse(tbls.contains("district"));
    }
    
    public void testDefaultMappings() throws Exception {
        File f = new File(json_path.getAbsolutePath());
        assertNotNull(f);
        assert (f.exists());
        JSONObject test_json = new JSONObject(FileUtil.readFile(f));
        PlannedPartitions p = new PlannedPartitions(catalogContext, test_json);
        p.setPartitionPhase("1");
        assertEquals(0, p.getPartitionId("warehouse", new Long(1)));
        assertEquals(1, p.getPartitionId("warehouse", new Long(2)));
        assertEquals(0, p.getPartitionId("district", new Long(1)));
        assertEquals(1, p.getPartitionId("district", new Long(2)));
        assertEquals(-1, p.getPartitionId("district", new Long(10)));
        assertEquals(0, p.getPartitionId("stock", new Long(1)));
        assertEquals(0, p.getPartitionId("item", new Long(10100)));
        assertEquals(1, p.getPartitionId("item", new Long(50100)));
    }

    public void testReconfigPlan() throws Exception {
        File f = new File(json_path.getAbsolutePath());
        assertNotNull(f);
        assert (f.exists());
        JSONObject test_json = new JSONObject(FileUtil.readFile(f));
        PlannedPartitions p = new PlannedPartitions(catalogContext, test_json);
        p.setPartitionPhase("1");
        ReconfigurationPlan plan = p.setPartitionPhase("2");
        assertNotNull(plan);
        List<ReconfigurationRange<? extends Comparable<?>>> out_ranges = plan.getOutgoing_ranges().get(0);
        System.out.println(out_ranges);
        assertEquals("Expected 9 outgoing ranges",9, out_ranges.size());
    }

}
