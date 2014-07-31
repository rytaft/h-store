package edu.brown.hashing;

import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONObject;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.hashing.PlannedPartitions.PartitionPhase;
import edu.brown.hashing.PlannedPartitions.PartitionRange;
import edu.brown.hashing.PlannedPartitions.PartitionedTable;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationTable;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;

public class TestTwoTieredRangePartitions extends BaseTestCase {

	Table catalog_tbl;
	
    public TestTwoTieredRangePartitions() {

    }

    public String test_json1 = "{" 
	+ "       \"default_table\":\"usertable\"," 
	+ "       \"partition_plan\":{" 
	+ "          \"tables\":{" 
	+ "            \"usertable\":{"
	+ "              \"partitions\":{" 
	+ "                1 : \"1-100\"," 
	+ "                2 : \"100-300\"," 
	+ "                3 : \"300-301,350-400,302-303\","
	+ "                4 : \"301-302,303-304,304-350\"       " 
	+ "              }     " 
	+ "            }" 
	+ "          }" 
	+ "        }" 
	+ "}";

    public String test_json2 = "{"
        + "       \"default_table\":\"usertable\","
        + "       \"partition_plan\":{"
        + "          \"tables\":{"
        + "            \"usertable\":{"
        + "              \"partitions\":{"
        + "                1 : \"1-400\","
        + "              }     "
        + "            }"
        + "          }"
        + "        }"
        + "}";

    private File json_path1;
    private File json_path2;

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.YCSB);
        this.catalog_tbl = this.getTable("USERTABLE");
        Column catalog_col = this.getColumn(catalog_tbl, "YCSB_KEY");
        catalog_tbl.setPartitioncolumn(catalog_col);
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
        assertEquals(1, p.getPartitionId("usertable", new Long[]{99L}));
        assertEquals(2, p.getPartitionId("usertable", new Long[]{100L}));
    }

    public void testExtractTableNames() throws Exception {
        JSONObject test_json = new JSONObject(test_json1);
        ExplicitPartitions p = new TwoTieredRangePartitions(catalogContext, test_json);
        Set<String> tbls = p.getExplicitPartitionedTables(test_json);
        assertTrue(tbls.contains("usertable"));
    }

    public void testBuildTablePartitions() throws Exception {
        JSONObject test_json = new JSONObject(test_json1);
        ExplicitPartitions p = new TwoTieredRangePartitions(catalogContext, test_json);
        p.setPartitionPlan(json_path1);
        assertEquals(1, p.getPartitionId("usertable", new Long[]{2L}));
        assertEquals(1, p.getPartitionId("usertable", new Long[]{1L}));
        assertEquals(1, p.getPartitionId("usertable", new Long[]{99L}));
        assertEquals(2, p.getPartitionId("usertable", new Long[]{100L}));
        assertEquals(2, p.getPartitionId("usertable", new Long[]{157L}));
        assertEquals(2, p.getPartitionId("usertable", new Long[]{299L}));
        assertEquals(3, p.getPartitionId("usertable", new Long[]{300L}));
        assertEquals(3, p.getPartitionId("usertable", new Long[]{350L}));
        assertEquals(3, p.getPartitionId("usertable", new Long[]{399L}));
        assertEquals(3, p.getPartitionId("usertable", new Long[]{302L}));
        assertEquals(4, p.getPartitionId("usertable", new Long[]{301L}));
        assertEquals(4, p.getPartitionId("usertable", new Long[]{303L}));
        assertEquals(4, p.getPartitionId("usertable", new Long[]{304L}));
        assertEquals(4, p.getPartitionId("usertable", new Long[]{340L}));
        assertEquals(-1, p.getPartitionId("usertable", new Long[]{0L}));
        assertEquals(-1, p.getPartitionId("usertable", new Long[]{54521L}));

        p.setPartitionPlan(json_path2);
        assertEquals(1, p.getPartitionId("usertable", new Long[]{2L}));
        assertEquals(1, p.getPartitionId("usertable", new Long[]{1L}));
        assertEquals(1, p.getPartitionId("usertable", new Long[]{99L}));
        assertEquals(1, p.getPartitionId("usertable", new Long[]{100L}));
        assertEquals(1, p.getPartitionId("usertable", new Long[]{157L}));
        assertEquals(1, p.getPartitionId("usertable", new Long[]{299L}));
        assertEquals(1, p.getPartitionId("usertable", new Long[]{300L}));
        assertEquals(1, p.getPartitionId("usertable", new Long[]{350L}));
        assertEquals(1, p.getPartitionId("usertable", new Long[]{399L}));
        assertEquals(1, p.getPartitionId("usertable", new Long[]{302L}));
        assertEquals(1, p.getPartitionId("usertable", new Long[]{301L}));
        assertEquals(1, p.getPartitionId("usertable", new Long[]{303L}));
        assertEquals(1, p.getPartitionId("usertable", new Long[]{304L}));
        assertEquals(1, p.getPartitionId("usertable", new Long[]{340L}));
        assertEquals(-1, p.getPartitionId("usertable", new Long[]{0L}));
        assertEquals(-1, p.getPartitionId("usertable", new Long[]{54521L}));
    }

    public void testPartitionRangeCompare() throws Exception {
        PartitionRange pr1_4 = new PartitionRange(catalog_tbl, 1, "1-4");
        PartitionRange pr1_4b = new PartitionRange(catalog_tbl, 1, "1-4");
        PartitionRange pr1_20 = new PartitionRange(catalog_tbl, 1, "1-20");
        PartitionRange pr2_3 = new PartitionRange(catalog_tbl, 1, "2-3");
        PartitionRange pr2_4 = new PartitionRange(catalog_tbl, 1, "2-4");
        PartitionRange pr3_3 = new PartitionRange(catalog_tbl, 1, "3-3");
        PartitionRange pr20_300 = new PartitionRange(catalog_tbl, 1, "20-300");
        PartitionRange pr40_50 = new PartitionRange(catalog_tbl, 1, "40-50");
        boolean exceptionCaught = false;
        try {
            PartitionRange pr5_3 = new PartitionRange(catalog_tbl, 1, "5-3");
        } catch (ParseException ex) {
            exceptionCaught = true;
        }
        assertTrue(exceptionCaught);
        assertTrue(pr1_4.compareTo(pr1_4b) == 0);
        assertTrue(pr1_4.compareTo(pr1_20) < 0);
        assertTrue(pr1_4.compareTo(pr2_3) < 0);
        assertTrue(pr2_3.compareTo(pr1_4) > 0);
        assertTrue(pr2_3.compareTo(pr2_4) < 0);
        assertTrue(pr2_3.compareTo(pr3_3) < 0);
        assertTrue(pr2_4.compareTo(pr3_3) < 0);
        assertTrue(pr20_300.compareTo(pr40_50) < 0);
        assertTrue(pr40_50.compareTo(pr20_300) > 0);
        assertTrue(pr40_50.compareTo(pr2_4) > 0);

    }

    public void testReconfigurationTable1() throws Exception {
        List<PartitionRange> olds = new ArrayList<>();
        List<PartitionRange> news = new ArrayList<>();

        olds.add(new PartitionRange(catalog_tbl, 1, "1-10"));
        olds.add(new PartitionRange(catalog_tbl, 2, "10-20"));
        olds.add(new PartitionRange(catalog_tbl, 3, "20-30"));
        PartitionedTable old_table = new PartitionedTable(olds, "table", catalog_tbl);

        news.add(new PartitionRange(catalog_tbl, 1, "1-5"));
        news.add(new PartitionRange(catalog_tbl, 2, "5-7"));
        news.add(new PartitionRange(catalog_tbl, 3, "7-10"));
        news.add(new PartitionRange(catalog_tbl, 2, "10-25"));
        news.add(new PartitionRange(catalog_tbl, 1, "25-26"));
        news.add(new PartitionRange(catalog_tbl, 3, "26-30"));
        PartitionedTable new_table = new PartitionedTable(news, "table", catalog_tbl);

        ReconfigurationTable reconfig = new ReconfigurationTable(catalogContext, old_table, new_table);
        ReconfigurationRange range = null;
        range = reconfig.getReconfigurations().get(0);
        assertEquals(5L, range.getMinIncl().get(0)[0]);
        assertEquals(7L, range.getMaxExcl().get(0)[0]);
        assertEquals(1, range.getOldPartition());
        assertEquals(2, range.getNewPartition());

        range = reconfig.getReconfigurations().get(1);
        assertEquals(7L, range.getMinIncl().get(0)[0]);
        assertEquals(10L, range.getMaxExcl().get(0)[0]);
        assertEquals(1, range.getOldPartition());
        assertEquals(3, range.getNewPartition());

        range = reconfig.getReconfigurations().get(2);
        assertEquals(20L, range.getMinIncl().get(0)[0]);
        assertEquals(25L, range.getMaxExcl().get(0)[0]);
        assertEquals(3, range.getOldPartition());
        assertEquals(2, range.getNewPartition());

        range = reconfig.getReconfigurations().get(3);
        assertEquals(25L, range.getMinIncl().get(0)[0]);
        assertEquals(26L, range.getMaxExcl().get(0)[0]);
        assertEquals(3, range.getOldPartition());
        assertEquals(1, range.getNewPartition());
        
    }

    public void testPreviousPlan() throws Exception {
        // TODO ae assertTrue(false);
    }

    public void testReconfigurationTable2() throws Exception {
        List<PartitionRange> olds = new ArrayList<>();
        List<PartitionRange> news = new ArrayList<>();

        olds.add(new PartitionRange(catalog_tbl, 1, "1-30"));
        PartitionedTable old_table = new PartitionedTable(olds, "table", catalog_tbl);

        news.add(new PartitionRange(catalog_tbl, 1, "1-10"));
        news.add(new PartitionRange(catalog_tbl, 2, "10-20"));
        news.add(new PartitionRange(catalog_tbl, 3, "20-30"));
        PartitionedTable new_table = new PartitionedTable(news, "table", catalog_tbl);

        ReconfigurationTable reconfig = new ReconfigurationTable(catalogContext, old_table, new_table);
        ReconfigurationRange range = null;
        range = reconfig.getReconfigurations().get(0);
        assertEquals(10L, range.getMinIncl().get(0)[0]);
        assertEquals(20L, range.getMaxExcl().get(0)[0]);
        assertEquals(1, range.getOldPartition());
        assertEquals(2, range.getNewPartition());

        range = reconfig.getReconfigurations().get(1);
        assertEquals(20L, range.getMinIncl().get(0)[0]);
        assertEquals(30L, range.getMaxExcl().get(0)[0]);
        assertEquals(1, range.getOldPartition());
        assertEquals(3, range.getNewPartition());
        
    }

    public void testReconfigurationTable3() throws Exception {
        List<PartitionRange> olds = new ArrayList<>();
        List<PartitionRange> news = new ArrayList<>();

        olds.add(new PartitionRange(catalog_tbl, 1, "1-30"));
        PartitionedTable old_table = new PartitionedTable(olds, "table", catalog_tbl);

        news.add(new PartitionRange(catalog_tbl, 1, "1-10"));
        news.add(new PartitionRange(catalog_tbl, 2, "10-20"));
        news.add(new PartitionRange(catalog_tbl, 3, "20-30"));
        PartitionedTable new_table = new PartitionedTable(news, "table", catalog_tbl);

        // REVERSED OLD <--> NEW
        ReconfigurationTable reconfig = new ReconfigurationTable(catalogContext, new_table, old_table);
        ReconfigurationRange range = null;
        range = reconfig.getReconfigurations().get(0);
        assertEquals(10L, range.getMinIncl().get(0)[0]);
        assertEquals(20L, range.getMaxExcl().get(0)[0]);
        assertEquals(2, range.getOldPartition());
        assertEquals(1, range.getNewPartition());

        range = reconfig.getReconfigurations().get(1);
        assertEquals(20L, range.getMinIncl().get(0)[0]);
        assertEquals(30L, range.getMaxExcl().get(0)[0]);
        assertEquals(3, range.getOldPartition());
        assertEquals(1, range.getNewPartition());
        
    }

    @SuppressWarnings("unchecked")
    public void testReconfigurationPlan() throws Exception {
        List<PartitionRange> olds = new ArrayList<>();
        List<PartitionRange> news = new ArrayList<>();

        olds.add(new PartitionRange(catalog_tbl, 1, "1-30"));
        PartitionedTable old_table = new PartitionedTable(olds, "table", catalog_tbl);
        Map<String, PartitionedTable> old_table_map = new HashMap<String, PlannedPartitions.PartitionedTable>();
        old_table_map.put("table", old_table);
        PartitionPhase old_phase = new PartitionPhase(old_table_map);

        news.add(new PartitionRange(catalog_tbl, 1, "1-10"));
        news.add(new PartitionRange(catalog_tbl, 2, "10-20"));
        news.add(new PartitionRange(catalog_tbl, 3, "20-30"));
        PartitionedTable new_table = new PartitionedTable(news, "table", catalog_tbl);
        Map<String, PartitionedTable> new_table_map = new HashMap<String, PlannedPartitions.PartitionedTable>();
        new_table_map.put("table", new_table);
        PartitionPhase new_phase = new PartitionPhase(new_table_map);

        ReconfigurationPlan reconfig_plan = new ReconfigurationPlan(catalogContext, old_phase, new_phase);

        ReconfigurationTable reconfig = (ReconfigurationTable) reconfig_plan.tables_map.get("table");
        ReconfigurationRange range = null;
        range = reconfig.getReconfigurations().get(0);
        assertEquals(10L, range.getMinIncl().get(0)[0]);
        assertEquals(20L, range.getMaxExcl().get(0)[0]);
        assertEquals(1, range.getOldPartition());
        assertEquals(2, range.getNewPartition());
        
        range = reconfig.getReconfigurations().get(1);
        assertEquals(20L, range.getMinIncl().get(0)[0]);
        assertEquals(30L, range.getMaxExcl().get(0)[0]);
        assertEquals(1, range.getOldPartition());
        assertEquals(3, range.getNewPartition());
        
        range = (ReconfigurationRange) reconfig_plan.incoming_ranges.get(2).get(0);
        assertEquals(10L, range.getMinIncl().get(0)[0]);
        assertEquals(20L, range.getMaxExcl().get(0)[0]);
        assertEquals(1, range.getOldPartition());
        assertEquals(2, range.getNewPartition());
        
        range = (ReconfigurationRange) reconfig_plan.outgoing_ranges.get(1).get(0);
        assertEquals(10L, range.getMinIncl().get(0)[0]);
        assertEquals(20L, range.getMaxExcl().get(0)[0]);
        assertEquals(1, range.getOldPartition());
        assertEquals(2, range.getNewPartition());
        
        range = (ReconfigurationRange) reconfig_plan.outgoing_ranges.get(1).get(1);
        assertEquals(20L, range.getMinIncl().get(0)[0]);
        assertEquals(30L, range.getMaxExcl().get(0)[0]);
        assertEquals(1, range.getOldPartition());
        assertEquals(3, range.getNewPartition());
        
        range = (ReconfigurationRange) reconfig_plan.incoming_ranges.get(3).get(0);
        assertEquals(20L, range.getMinIncl().get(0)[0]);
        assertEquals(30L, range.getMaxExcl().get(0)[0]);
        assertEquals(1, range.getOldPartition());
        assertEquals(3, range.getNewPartition());
    }
}
