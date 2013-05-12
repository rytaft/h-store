/**
 * 
 */
package edu.brown.hstore.reconfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.voltdb.exceptions.ReconfigurationException;
import org.voltdb.exceptions.ReconfigurationException.ExceptionTypes;

import edu.brown.hashing.PlannedPartitions;
import edu.brown.hashing.ReconfigurationPlan;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Class to track the reconfiguration state and progress for a single partition
 * Not thread safe
 * @author aelmore
 *
 */
public class ReconfigurationTracking implements ReconfigurationTrackingInterface {

    private static final Logger LOG = Logger.getLogger(ReconfigurationTracking.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    private List<ReconfigurationRange<? extends Comparable<?>>> outgoing_ranges;
    private List<ReconfigurationRange<? extends Comparable<?>>> incoming_ranges;
    public List<ReconfigurationRange<? extends Comparable<?>>> dataMigratedOut;
    public List<ReconfigurationRange<? extends Comparable<?>>> dataMigratedIn;
    
    public Map<String,Set<Comparable>> migratedIn;
    public Map<String,Set<Comparable>> migratedOut;
    
    private int partition_id;
    private PlannedPartitions partitionPlan;
    
    
    public ReconfigurationTracking(PlannedPartitions partitionPlan,List<ReconfigurationRange<? extends Comparable<?>>> outgoing_ranges,
            List<ReconfigurationRange<? extends Comparable<?>>> incoming_ranges, int partition_id) {
        super();
        this.partitionPlan = partitionPlan;
        this.outgoing_ranges = new ArrayList<ReconfigurationRange<? extends Comparable<?>>>();
        if (outgoing_ranges!= null)
            this.outgoing_ranges.addAll(outgoing_ranges);
        this.incoming_ranges = new ArrayList<ReconfigurationRange<? extends Comparable<?>>>();
        if (incoming_ranges != null)
            this.incoming_ranges.addAll(incoming_ranges);
        this.partition_id = partition_id;
        this.migratedIn = new HashMap<String, Set<Comparable>>();
        this.migratedOut = new HashMap<String, Set<Comparable>>();
    }
    
    public ReconfigurationTracking(PlannedPartitions partitionPlan, ReconfigurationPlan plan, int partition_id){
        this(partitionPlan,plan.getOutgoing_ranges().get(partition_id), plan.getIncoming_ranges().get(partition_id),partition_id);
        /*this.partitionPlan = partitionPlan;
        this.outgoing_ranges = new ArrayList<ReconfigurationRange<? extends Comparable<?>>>();
        if(plan.getOutgoing_ranges().containsKey(partition_id))
            this.outgoing_ranges.addAll(plan.getOutgoing_ranges().get(partition_id));
        this.incoming_ranges = new ArrayList<ReconfigurationRange<? extends Comparable<?>>>();
        if(plan.getIncoming_ranges().containsKey(partition_id))
            this.incoming_ranges.addAll(plan.getIncoming_ranges().get(partition_id));
        this.partition_id = partition_id;*/       
    }
    
    @Override 
    public boolean markRangeAsMigratedOut(List<ReconfigurationRange<? extends Comparable<?>>> range ){
        throw new NotImplementedException();
    }
    
    @Override
    public boolean markRangeAsReceived(List<ReconfigurationRange<? extends Comparable<?>>> range ){

        throw new NotImplementedException();
    }
    
    @SuppressWarnings({ "unused", "rawtypes", "unchecked" })
    private boolean markAsMigrated(Map<String,Set<Comparable>> migratedMapSet, String table_name, Object key){
        if(migratedMapSet.containsKey(table_name) == false){
            migratedMapSet.put(table_name, new HashSet());
        }
        assert(key instanceof Comparable);
        return migratedMapSet.get(table_name).add((Comparable) key);
    }

    private boolean checkMigratedMapSet(Map<String,Set<Comparable>> migratedMapSet, String table_name, Object key){
        if(migratedMapSet.containsKey(table_name) == false){
           return false;
        }
        return migratedMapSet.get(table_name).contains((Comparable)key);
    }
    
    @Override
    public boolean markKeyAsMigratedOut(String table_name, Object key) {
        return markAsMigrated(migratedOut, table_name, key);
    }

    @Override
    public boolean markKeyAsReceived(String table_name, Object key) {
        return markAsMigrated(migratedIn, table_name, key);
    }

    @Override
    public boolean checkKeyOwned(String table_name, Object key) throws ReconfigurationException {
        
            int expectedPartition;
            int previousPartition;
            try {
                expectedPartition = partitionPlan.getPartitionId(table_name, key);
                previousPartition =  partitionPlan.getPreviousPartitionId(table_name, key);        
            } catch (Exception e) {
                LOG.error("Exception trying to get partition IDs",e);
                throw new RuntimeException(e);
            }   
            if (expectedPartition == partition_id &&  previousPartition == partition_id)
            {
                //This partition should have the key and it didnt move     
                if (debug.val) LOG.debug(String.format("Key %s is at %s and did not migrate ",key,partition_id));
                return true;
            } else if (expectedPartition == partition_id &&  previousPartition != partition_id) {
                //Key should be moving to here
                if (debug.val) LOG.debug(String.format("Key %s should be at %s. Checking if migrated in",key,partition_id));
                if (checkMigratedMapSet(migratedIn,table_name,key)== true){
                    return true;
                } else {                
                    //  TODO check ranges if false
                    ReconfigurationException ex = new ReconfigurationException(ExceptionTypes.TUPLES_NOT_MIGRATED,table_name, previousPartition,expectedPartition,(Comparable) key);
                    throw ex;
                }
                
            } else if (expectedPartition != partition_id &&  previousPartition == partition_id) {
                //Key should be moving away
                if (debug.val) LOG.debug(String.format("Key %s was at %s. Checking if migrated ",key,partition_id));
                if (checkMigratedMapSet(migratedOut,table_name,key)){

                    //TODO check ranges
                    ReconfigurationException ex = new ReconfigurationException(ExceptionTypes.TUPLES_MIGRATED_OUT,table_name, previousPartition,expectedPartition,(Comparable) key);
                    throw ex;
                }
                return true;
            } else if (expectedPartition != partition_id &&  previousPartition != partition_id) {
                //We didnt have nor are are expected to
                if (debug.val) LOG.debug(String.format("Key %s is not at %s, nor was it previously",key,partition_id));

                return false;
            }
            
        
        return false;
    }
    
    
}
