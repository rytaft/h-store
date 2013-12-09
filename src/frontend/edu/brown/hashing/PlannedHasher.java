/**
 * 
 */
package edu.brown.hashing;

import org.json.JSONObject;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.utils.NotImplementedException;

import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.reconfiguration.ReconfigurationCoordinator;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.FileUtil;

/**
 * @author aelmore Hasher that uses a planned partition plan, stored in the
 *         database catalog. This partition plan can change over time
 */
public class PlannedHasher extends DefaultHasher {
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    public static final String YCSB_TEST = "YCSB_TEST";

    String ycsb_plan = "{"+
            "       \"default_table\":\"usertable\"," +        
            "       \"partition_plans\":{"+
            "          \"1\" : {"+
            "            \"tables\":{"+
            "              \"usertable\":{"+
            "                \"partitions\":{"+
            "                  0 : \"0-100000\""+
            "                }     "+
            "              }"+
            "            }"+
            "          },"+
            "          \"2\" : {"+
            "            \"tables\":{"+
            "              \"usertable\":{"+
            "                \"partitions\":{"+
            "                  0 : \"0-50000\","+
            "                  1 : \"50000-100000\""+
            "                }     "+
            "              }"+
            "            }"+
            "          },"+
            "          \"3\" : {"+
            "            \"tables\":{"+
            "              \"usertable\":{"+
            "                \"partitions\":{"+
            "                  0 : \"0-95000\","+
            "                  1 : \"95000-100000\""+
            "                }     "+
            "              }"+
            "            }"+
            "          }"+
            "        }"+
            "}";
    
    private PlannedPartitions planned_partitions = null;

    private ReconfigurationCoordinator reconfigCoord = null;
    /**
     * Update the current partition plan
     * 
     * @param partition_plan
     * @return The delta, or null if there was no change
     * @throws Exception
     */
    public ReconfigurationPlan changePartitionPhase(String partition_plan) throws Exception {
        return planned_partitions.setPartitionPhase(partition_plan);
    }

    /**
     * @param catalog_db
     * @param num_partitions
     */
    public PlannedHasher(CatalogContext catalogContext, int num_partitions, HStoreConf hstore_conf) {
        super(catalogContext, num_partitions,hstore_conf);
        try {
            JSONObject partition_json = null;
            if(hstore_conf != null && hstore_conf.global.hasher_plan.equalsIgnoreCase(YCSB_TEST)){
                LOG.info("Using YCSB test plan");
                partition_json = new JSONObject(ycsb_plan);
            } else if(hstore_conf != null && hstore_conf.global.hasher_plan != null){
                LOG.info("Attempting to use partition plan at : " + hstore_conf.global.hasher_plan);
                partition_json = new JSONObject(FileUtil.readFile(hstore_conf.global.hasher_plan));
            } else {
                LOG.error(" *** Using a planned hasher without a specified partition plan. Using YCSB default *** ");
                partition_json = new JSONObject(ycsb_plan);
            }
            
            planned_partitions = new PlannedPartitions(catalogContext, partition_json);
        } catch (Exception ex) {
            LOG.error("Error intializing planned partitions", ex);
            throw new RuntimeException(ex);
        }
    }

    /**
     * @param catalog_db
     */
    public PlannedHasher(CatalogContext catalogContext) {
        super(catalogContext);
    }

    @Override
    public int hash(Object value) {
        throw new NotImplementedException("Hashing without Catalog not supported");
    }

    @Override
    public int hash(Object value, CatalogType catalogItem) {
        if (catalogItem instanceof Column || catalogItem instanceof Procedure || catalogItem instanceof Statement) {
            try {
                //If we do not have an RC, or there is an RC but no reconfig is in progress
                if(reconfigCoord == null || ReconfigurationCoordinator.FORCE_DESTINATION || (reconfigCoord != null && !reconfigCoord.getReconfigurationInProgress())){
                    if (debug.val) LOG.debug(String.format("\t%s Id:%s Partition:%s Phase:%s",catalogItem,value,planned_partitions.getPartitionId(catalogItem, value),planned_partitions.getCurrent_phase()));
                    return planned_partitions.getPartitionId(catalogItem, value);
                } else {
                    int expectedPartition = planned_partitions.getPartitionId(catalogItem, value);
                    int previousPartition = planned_partitions.getPreviousPartitionId(catalogItem, value);
                    if (expectedPartition == previousPartition) {
                        //The item isn't moving
                        return expectedPartition;
                    } else {
                        //the item is moving
                        //check with RC on which partition. 
                        return reconfigCoord.getPartitionId(previousPartition, expectedPartition, catalogItem, value);
                    }
                }
            } catch (Exception e) {
                LOG.error("Error on looking up partitionId from planned partition", e);
                throw new RuntimeException(e);
            }
        }
        throw new NotImplementedException("TODO");
    }

    @Override
    public int hash(Object value, int num_partitions) {
        throw new NotImplementedException("Hashing without Catalog not supported");
    }

    public synchronized PlannedPartitions getPlanned_partitions() {
        return planned_partitions;
    }

    public void setReconfigCoord(ReconfigurationCoordinator reconfigCoord) {
        this.reconfigCoord = reconfigCoord;
    }

}
