/**
 * 
 */
package edu.brown.hashing;

import org.json.JSONObject;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
 * @author rytaft, aelmore
 */
public class TwoTieredRangeHasher extends DefaultHasher implements ExplicitHasher {
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    public static final String YCSB_TEST = "YCSB_TEST";

    String ycsb_plan = "{"+
            "       \"default_table\":\"usertable\"," +        
            "       \"partition_plan\":{"+
            "         \"tables\":{"+
            "           \"usertable\":{"+
            "             \"partitions\":{"+
            "                0 : \"0-100000\""+
            "             }"+
            "           }"+
            "         }"+
            "       }"+
            "}";
    
    private ExplicitPartitions partitions = null;

    private ReconfigurationCoordinator reconfigCoord = null;
    /**
     * Update the current partition plan
     * 
     * @param partition_plan - the new plan
     * @return The delta, or null if there was no change
     * @throws Exception
     */
    public ReconfigurationPlan changePartitionPlan(String partition_plan) throws Exception {
    	try {
            if(partition_plan != null) {
            	JSONObject partition_json = new JSONObject(partition_plan);
            	return partitions.setPartitionPlan(partition_json);
            } else {
                LOG.error("Attempt to change partition plan without plan file");
            } 
        } catch (Exception ex) {
            LOG.error("Error changing partitions", ex);
            throw new RuntimeException(ex);
        }
        return null;
    }

    /**
     * @param catalog_db
     * @param num_partitions
     */
    public TwoTieredRangeHasher(CatalogContext catalogContext, int num_partitions, HStoreConf hstore_conf) {
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
                LOG.error(" *** Using a two-tiered range hasher without a specified partition plan. Using YCSB default *** ");
                partition_json = new JSONObject(ycsb_plan);
            }
            
            partitions = new TwoTieredRangePartitions(catalogContext, partition_json);
        } catch (Exception ex) {
            LOG.error("Error intializing partitions", ex);
            throw new RuntimeException(ex);
        }
    }

    /**
     * @param catalog_db
     */
    public TwoTieredRangeHasher(CatalogContext catalogContext) {
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
            	List<CatalogType> catalogList = Arrays.asList(catalogItem);
            	List<Object> valueList = Arrays.asList(value);
                //If we do not have an RC, or there is an RC but no reconfig is in progress
                if(reconfigCoord == null || ReconfigurationCoordinator.FORCE_DESTINATION || (reconfigCoord != null && !this.inReconfiguration.get())){
                	if (debug.val) LOG.debug(String.format("\t%s Id:%s Partition:%s",catalogItem,value,partitions.getPartitionId(catalogList, valueList)));
                    return partitions.getPartitionId(catalogList, valueList);
                } else {
                    int expectedPartition = partitions.getPartitionId(catalogList, valueList);
                    int previousPartition = partitions.getPreviousPartitionId(catalogList, valueList);
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
    
    public int hash(List<Object> values, List<CatalogType> catalogItems) {
    	for (CatalogType catalogItem : catalogItems) {
    		if (!(catalogItem instanceof Column) && !(catalogItem instanceof Procedure) && !(catalogItem instanceof Statement)) {
    			throw new NotImplementedException("TODO");
    		}
    	}
    
        try {
            //If we do not have an RC, or there is an RC but no reconfig is in progress
            if(reconfigCoord == null || ReconfigurationCoordinator.FORCE_DESTINATION || (reconfigCoord != null && !this.inReconfiguration.get())){
            	if (debug.val) LOG.debug(String.format("\t%s Id:%s Partition:%s",catalogItems.get(0),values.get(0),partitions.getPartitionId(catalogItems, values)));
                return partitions.getPartitionId(catalogItems, values);
            } else {
                int expectedPartition = partitions.getPartitionId(catalogItems, values);
                int previousPartition = partitions.getPreviousPartitionId(catalogItems, values);
                if (expectedPartition == previousPartition) {
                    //The item isn't moving
                    return expectedPartition;
                } else {
                    //the item is moving
                    //check with RC on which partition. 
                    return reconfigCoord.getPartitionId(previousPartition, expectedPartition, catalogItems, values);
                }
            }
        } catch (Exception e) {
            LOG.error("Error on looking up partitionId from planned partition", e);
            throw new RuntimeException(e);
        }
        
    }

    @Override
    public int hash(Object value, int num_partitions) {
        throw new NotImplementedException("Hashing without Catalog not supported");
    }

    @Override
    public AbstractHasher clone() {
    	return new TwoTieredRangeHasher(this.catalogContext, this.num_partitions, this.hstore_conf);
    }

    public synchronized ExplicitPartitions getPartitions() {
        return (ExplicitPartitions)partitions;
    }
    
    public Map<Integer, List<Integer>> getPartitionReplicas(){
        return partitions.getPartitionReplicas();
    }

    public void setReconfigCoord(ReconfigurationCoordinator reconfigCoord) {
        this.reconfigCoord = reconfigCoord;
    }
    
    @Override
    public ReconfigurationPlan changePartitionPhase(String partition_plan) throws Exception {
        throw new NotImplementedException("TODO");
    }
    
    @Override
    public boolean hasMultiColumnRanges() {
    	return true;
    }

}
