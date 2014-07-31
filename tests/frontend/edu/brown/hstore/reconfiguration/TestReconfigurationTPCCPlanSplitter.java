/**
 * 
 */
package edu.brown.hstore.reconfiguration;

import java.io.File;
import java.util.Set;
import java.util.HashSet;

import org.junit.Test;

import edu.brown.BaseTestCase;
import edu.brown.hashing.ExplicitPartitions;
import edu.brown.hashing.PlannedHasher;
import edu.brown.hashing.PlannedPartitions;
import edu.brown.hashing.ReconfigurationPlan;
import edu.brown.hashing.TwoTieredRangePartitions;
import edu.brown.hstore.reconfiguration.ReconfigurationUtil.ReconfigurationPair;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;

/**
 * @author aelmore
 */
public class TestReconfigurationTPCCPlanSplitter extends BaseTestCase {

    private File json_path1;

    @Test
    public void txestReconfigurationPair() throws Exception {
        ReconfigurationPair pair1 = new ReconfigurationPair(1, 2);
        ReconfigurationPair pair2 = new ReconfigurationPair(2, 1);
        ReconfigurationPair pair3 = new ReconfigurationPair(1, 3);
        ReconfigurationPair pair4 = new ReconfigurationPair(1, 2);
        assertTrue(pair1.compareTo(pair2) < 0);
        assertTrue(pair1.compareTo(pair3) > 0);
        assertTrue(pair1.compareTo(pair4) == 0);
        assertTrue(pair1.equals(pair4));
        assertFalse(pair2.equals(pair3));

        Set<ReconfigurationPair> reconfigPairs = new HashSet<>();
        reconfigPairs.add(pair1);
        reconfigPairs.add(pair2);
        reconfigPairs.add(pair3);
        reconfigPairs.add(pair4);
        assertTrue(reconfigPairs.size() == 3);

    }

    /**
     * 
     */
    @Test
    public void testReconfigurationPlanSplitter() throws Exception {
        ExplicitPartitions p = new PlannedPartitions(catalogContext, json_path1);
        ReconfigurationPlan new_plan = p.setPartitionPhase("1");
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);

        String tmp_dir = System.getProperty("java.io.tmpdir");
        json_path1 = FileUtil.join(tmp_dir, "test1.json");
        FileUtil.writeStringToFile(json_path1, plan1);
    }

    String plan1 = "{" +
            "  \"partition_plans\": {" +
            "    \"0\": {" +
            "      \"tables\": {" +
            "      \"district\": {" +
            "        \"partitions\": {" +
            "          \"0\": \"1-2\", " +
            "        }" +
            "      }," +
            "      \"warehouse\": {" +
            "        \"partitions\": {" +
            "          \"0\": \"1-2\", " +
            "        }      " +
            "      }" +
            "    }" +
            "   }," +
            "    \"1\": {" +
            "      \"tables\": {" +
            "          \"warehouse\": {" +
            "            \"partitions\": {" +
            "              \"1\": \"1-2\"," +
            "               " +
            "            }" +
            "          }," +
            "          \"district\": {" +
            "            \"partitions\": {" +
            "              \"1\": \"1-2\", " +
            "            }" +
            "          }" +
            "        }" +
            "    }" +
            "  }, " +
            "  \"default_table\": \"district\"" +
            "}";

}
