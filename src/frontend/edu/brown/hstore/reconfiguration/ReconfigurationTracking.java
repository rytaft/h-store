/**
 * 
 */
package edu.brown.hstore.reconfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.CatalogType;
import org.voltdb.exceptions.ReconfigurationException;
import org.voltdb.exceptions.ReconfigurationException.ExceptionTypes;
import org.voltdb.catalog.Table;

import edu.brown.hashing.ExplicitPartitions;
import edu.brown.hashing.ReconfigurationPlan;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Class to track the reconfiguration state and progress for a single partition
 * Not thread safe
 * @author aelmore, rytaft
 *
 */
public class ReconfigurationTracking implements ReconfigurationTrackingInterface {

    private static final Logger LOG = Logger.getLogger(ReconfigurationTracking.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    public static boolean PULL_SINGLE_KEY = true;
    private Map<String, TreeSet<ReconfigurationRange>> outgoing_ranges_map;
    private Map<String, TreeSet<ReconfigurationRange>> incoming_ranges_map;
    private Map<ReconfigurationRange, ReconfigurationRange> enclosing_range;
    public ConcurrentSkipListMap<ReconfigurationRange,String> dataMigratedOut;
    public ConcurrentSkipListMap<ReconfigurationRange,String> dataPartiallyMigratedOut;

    public ConcurrentSkipListMap<ReconfigurationRange,String> dataPartiallyMigratedIn;
    public ConcurrentSkipListMap<ReconfigurationRange,String> dataMigratedIn;
    private int rangesMigratedInCount = 0;
    private int rangesMigratedOutCount = 0;
    private int incomingRangesCount = 0;
    private int outgoingRangesCount = 0;

    //set of individual keys migrated out/in status, stored in a map by table name as key
    // TODO these should probably be concurrent data structures...
    public Map<String,Set<List<Object>>> migratedKeyIn;
    public Map<String,Set<List<Object>>> migratedKeyOut;

    private int partition_id;
    private ExplicitPartitions partitionPlan;

    private CatalogContext catalogContext;


    protected ReconfigurationTracking(ExplicitPartitions partitionPlan,Map<String, TreeSet<ReconfigurationRange>> outgoing_ranges,
            Map<String, TreeSet<ReconfigurationRange>> incoming_ranges, Map<ReconfigurationRange, ReconfigurationRange> enclosing_range,
            int partition_id, CatalogContext catalogContext) {
        super();
        this.partitionPlan = partitionPlan;
        this.catalogContext = catalogContext;
        this.outgoing_ranges_map = outgoing_ranges;
        this.incoming_ranges_map = incoming_ranges;
        this.enclosing_range = enclosing_range;
        if(this.incoming_ranges_map != null) {
            for(TreeSet<ReconfigurationRange> ranges : this.incoming_ranges_map.values()) {
                incomingRangesCount += ranges.size();
            }
        }
        if(this.outgoing_ranges_map != null) {
            for(TreeSet<ReconfigurationRange> ranges : this.outgoing_ranges_map.values()){
                outgoingRangesCount += ranges.size();
            }
        }
        this.partition_id = partition_id;
        this.migratedKeyIn = new HashMap<String, Set<List<Object>>>();
        this.migratedKeyOut = new HashMap<String, Set<List<Object>>>();
        this.dataMigratedIn = new ConcurrentSkipListMap<>();
        this.dataMigratedOut = new ConcurrentSkipListMap<>();
        this.dataPartiallyMigratedOut = new ConcurrentSkipListMap<>();
        this.dataPartiallyMigratedIn = new ConcurrentSkipListMap<>();
    }

    public ReconfigurationTracking(ExplicitPartitions partitionPlan, ReconfigurationPlan plan, int partition_id){
        this(partitionPlan,plan.getOutgoing_ranges_map().get(partition_id), 
                plan.getIncoming_ranges_map().get(partition_id), plan.getEnclosing_range_map(),
                partition_id, plan.getCatalogContext());
    }

    @Override 
    public boolean markRangeAsMigratedOut(ReconfigurationRange range ) throws ReconfigurationException{
        for (int i = 0; i < range.getMinIncl().size() && i < range.getMaxExcl().size(); ++i) {
            // Every ReconfigurationRange in dataMigratedOut should contain exactly one contiguous range 
            // (i.e. one min and one max value) so that findReconfigurationRangeConcurrent will be correct
            ReconfigurationRange new_range = new ReconfigurationRange(range.getTableName(), range.getKeySchema(), 
                    range.getMinIncl().get(i), range.getMaxExcl().get(i), range.getOldPartition(), range.getNewPartition());
            this.dataMigratedOut.put(new_range, range.getTableName());
        }

        rangesMigratedOutCount++;
        if(rangesMigratedOutCount==outgoingRangesCount){
            throw new ReconfigurationException(ExceptionTypes.ALL_RANGES_MIGRATED_OUT);
        }

        return true;
    }

    @Override 
    public boolean markRangeAsPartiallyMigratedOut(ReconfigurationRange range ) throws ReconfigurationException{
        for (int i = 0; i < range.getMinIncl().size() && i < range.getMaxExcl().size(); ++i) {
            // Every ReconfigurationRange in dataPartiallyMigratedOut should contain exactly one contiguous range 
            // (i.e. one min and one max value) so that findReconfigurationRangeConcurrent will be correct
            ReconfigurationRange new_range = new ReconfigurationRange(range.getTableName(), range.getKeySchema(), 
                    range.getMinIncl().get(i), range.getMaxExcl().get(i), range.getOldPartition(), range.getNewPartition());
            this.dataPartiallyMigratedOut.put(new_range, range.getTableName());
        }

        return true;
    }


    @Override 
    public boolean markRangeAsMigratedOut(List<ReconfigurationRange> ranges ){
        boolean allAdded = true;
        for(ReconfigurationRange range : ranges){
            boolean res = this.markRangeAsMigratedOut(range);
            if (!res)
                allAdded =false;
        }
        return allAdded;
    }

    @Override
    public boolean markRangeAsReceived(List<ReconfigurationRange> ranges ){
        boolean allAdded = true;
        for(ReconfigurationRange range : ranges){
            boolean res = this.markRangeAsReceived(range);
            if (!res)
                allAdded =false;
        }
        return allAdded;
    }

    @Override
    public boolean markRangeAsReceived(ReconfigurationRange range ){
        for (int i = 0; i < range.getMinIncl().size() && i < range.getMaxExcl().size(); ++i) {
            // Every ReconfigurationRange in dataMigratedIn should contain exactly one contiguous range 
            // (i.e. one min and one max value) so that findReconfigurationRangeConcurrent will be correct
            ReconfigurationRange new_range = new ReconfigurationRange(range.getTableName(), range.getKeySchema(), 
                    range.getMinIncl().get(i), range.getMaxExcl().get(i), range.getOldPartition(), range.getNewPartition());
            this.dataMigratedIn.put(new_range, range.getTableName());
        }

        rangesMigratedInCount++;
        //FIXME this is not tracking properly
        LOG.info("marked, ranges to go:" + (incomingRangesCount-rangesMigratedInCount) );
        if(rangesMigratedInCount==incomingRangesCount){
            throw new ReconfigurationException(ExceptionTypes.ALL_RANGES_MIGRATED_IN);
        }

        return true;
    }

    @Override
    public boolean markRangeAsPartiallyReceived(ReconfigurationRange range ){
        for (int i = 0; i < range.getMinIncl().size() && i < range.getMaxExcl().size(); ++i) {
            // Every ReconfigurationRange in dataPartiallyMigratedIn should contain exactly one contiguous range 
            // (i.e. one min and one max value) so that findReconfigurationRangeConcurrent will be correct
            ReconfigurationRange new_range = new ReconfigurationRange(range.getTableName(), range.getKeySchema(), 
                    range.getMinIncl().get(i), range.getMaxExcl().get(i), range.getOldPartition(), range.getNewPartition());
            this.dataPartiallyMigratedIn.put(new_range, range.getTableName());
        }

        return true;
    }


    private boolean markAsMigrated(Map<String,Set<List<Object>>> migratedMapSet, String table_name, List<Object> key){
        if(migratedMapSet.containsKey(table_name) == false){
            migratedMapSet.put(table_name, new HashSet<List<Object>>());
        }
        return migratedMapSet.get(table_name).add(key);

    }

    private boolean checkMigratedMapSet(Map<String,Set<List<Object>>> migratedMapSet, String table_name, Object key){
        if(migratedMapSet.containsKey(table_name) == false){
            if (trace.val) LOG.trace("Checking a key for which there is no table tracking for yet " + table_name); 
            return false;
        }
        return migratedMapSet.get(table_name).contains(key);
    }

    @Override
    public boolean markKeyAsMigratedOut(String table_name, List<Object> key) {
        Set<ReconfigurationRange> ranges;
        try {
            ranges = ReconfigurationPlan.findAllReconfigurationRanges(table_name, key, 
                    this.outgoing_ranges_map, catalogContext, enclosing_range);
        }
        catch (Exception e) {
            LOG.error("Exception markKeyAsMigratedOut",e);
            return false;
        }
        for (ReconfigurationRange range : ranges) {
            markRangeAsPartiallyMigratedOut(range);
        }
        return markAsMigrated(migratedKeyOut, table_name, key);
    }

    @Override
    public boolean markKeyAsReceived(String table_name, List<Object> key) {
        Set<ReconfigurationRange> ranges;
        try {
            ranges = ReconfigurationPlan.findAllReconfigurationRanges(table_name, key, 
                    this.incoming_ranges_map, catalogContext, enclosing_range);
        }
        catch (Exception e) {
            LOG.error("Exception markKeyAsReceived",e);
            return false;
        }
        for (ReconfigurationRange range : ranges) {
            markRangeAsPartiallyReceived(range);
        }
        return markAsMigrated(migratedKeyIn, table_name, key);
    }

    @Override
    public boolean checkKeyOwned(CatalogType catalog, Object key) throws ReconfigurationException{
        return checkKeyOwned(Arrays.asList(catalog), Arrays.asList(key));
    }

    @Override
    public boolean checkKeyOwned(List<CatalogType> catalog, List<Object> key) throws ReconfigurationException{

        Table table = this.partitionPlan.getTable(catalog);
        String tableName = table.getName().toLowerCase();    	
        if (trace.val) LOG.trace(String.format("Checking Key owned for catalog:%s table:%s",catalog.toString(),tableName));
        return checkKeyOwned(table, key);
    }

    @Override
    public boolean quickCheckKeyOwned(int previousPartition, int expectedPartition, CatalogType catalog, Object key) {
        return quickCheckKeyOwned(previousPartition, expectedPartition, Arrays.asList(catalog), Arrays.asList(key));
    }

    @Override
    public boolean quickCheckKeyOwned(int previousPartition, int expectedPartition, List<CatalogType> catalog, List<Object> key) {
        try{
            Table table = this.partitionPlan.getTable(catalog);
            String table_name = table.getName().toLowerCase();
            if (expectedPartition == partition_id &&  previousPartition == partition_id)
            {
                //This partition should have the key and it didnt move     
                if (trace.val) LOG.trace(String.format("Key %s is at %s and did not migrate ",key,partition_id));
                return true;
            } else if (expectedPartition == partition_id &&  previousPartition != partition_id) {
                //Key should be moving to here
                if (debug.val) LOG.debug(String.format("Key %s should be at %s. Checking if migrated in",key,partition_id));

                //Has the key been received as a single key
                if (checkMigratedMapSet(migratedKeyIn,table_name,key)== true){
                    if (debug.val) LOG.debug(String.format("Key has been migrated in %s (%s)",key,table_name));
                    return true;
                } else {                       
                    //check if the key was received in a range
                    ReconfigurationRange range = ReconfigurationPlan.findReconfigurationRangeConcurrent(table_name, key, this.dataMigratedIn, catalogContext);
                    if(range != null){
                        if (debug.val) LOG.debug(String.format("Key has been migrated in range %s %s (%s)",range, key,table_name));
                        return true;
                    }
                    return false;
                }
            } else if (expectedPartition != partition_id &&  previousPartition == partition_id) {
                //Key should be moving away
                if (debug.val) LOG.debug(String.format("Key %s was at %s. Checking if migrated ",key,partition_id));                
                //Check to see if we migrated this key individually
                if (checkMigratedMapSet(migratedKeyOut,table_name,key)){
                    if (debug.val) LOG.debug(String.format("Key has been migrated out %s (%s)",key,table_name));                    
                    return false;
                }                
                //check to see if this key was migrated in a range
                ReconfigurationRange range = ReconfigurationPlan.findReconfigurationRangeConcurrent(table_name, key, this.dataMigratedOut, catalogContext);
                if(range != null){
                    if (debug.val) LOG.debug(String.format("Key has been migrated out range %s %s (%s)",range, key,table_name));
                    return false;
                }               
                //check to see if this key was migrated in a range
                range = ReconfigurationPlan.findReconfigurationRangeConcurrent(table_name, key, this.dataPartiallyMigratedOut, catalogContext);
                if(range != null){
                    if (debug.val) LOG.debug(String.format("Key may have been migrated out in partially dirtied range %s %s (%s)",range, key,table_name));
                    return false;
                }               
                return true;
            } else if (expectedPartition != partition_id &&  previousPartition != partition_id) {
                //We didnt have nor are are expected to
                if (debug.val) LOG.debug(String.format("Key %s is not at %s, nor was it previously",key,partition_id));

                return false;
            }
        } catch (Exception e) {
            LOG.error("Exception quickCheckKeyOwned",e);
            return false;
        }
        LOG.error("Should never get here");
        return false;

    }

    @Override
    public boolean checkKeyOwned(Table table, List<Object> key) throws ReconfigurationException {

        String table_name = table.getName().toLowerCase();
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
            if (trace.val) LOG.trace(String.format("Key %s is at %s and did not migrate ",key,partition_id));
            return true;
        } else if (expectedPartition == partition_id &&  previousPartition != partition_id) {
            //Key should be moving to here
            if (debug.val) LOG.debug(String.format("Key %s should be at %s. Checking if migrated in",key,partition_id));

            //Has the key been received as a single key
            if (checkMigratedMapSet(migratedKeyIn,table_name,key)== true){
                if (debug.val) LOG.debug(String.format("Key has been migrated in %s (%s)",key,table_name));
                return true;
            } else {                       
                //check if the key was received in a range                            
                if(ReconfigurationPlan.findReconfigurationRangeConcurrent(table_name, key, this.dataMigratedIn, catalogContext) != null){
                    return true;
                }
                List<String> relatedTables = partitionPlan.getRelatedTables(table_name);
                if (debug.val && relatedTables != null){
                    LOG.debug(String.format("Table %s has related tables:%s",table_name, StringUtils.join(relatedTables, ',')));
                }
                // The key has not been received. Throw an exception to notify
                //It could be in a partial range, but that doesn't matter to us. Still need to pull the full range.
                if (PULL_SINGLE_KEY) {
                    ReconfigurationException ex = null;
                    if(relatedTables == null){
                        ex = new ReconfigurationException(ExceptionTypes.TUPLES_NOT_MIGRATED, table, previousPartition, expectedPartition, key);
                    } else {
                        List<Table> relatedTablesToPull = new ArrayList<>();
                        for(String rTableName : relatedTables){
                            Table rTable = this.partitionPlan.getCatalogContext().getTableByName(rTableName);
                            if (checkMigratedMapSet(migratedKeyIn,rTableName,key)== false) {
                                if (debug.val) LOG.debug(" Related table key not migrated, adding to pull : " + rTable);
                                relatedTablesToPull.add(rTable);
                            } else {
                                if (debug.val) LOG.debug(" Related table already pulled for key " + rTable);
                            }
                        }
                        ex = new ReconfigurationException(ExceptionTypes.TUPLES_NOT_MIGRATED, relatedTablesToPull, previousPartition, expectedPartition, key);
                    }
                    throw ex;
                } else {
                    List<ReconfigurationRange> rangesToPull = new ArrayList<>();  
                    try {
                        if(relatedTables == null) {
                            Set<ReconfigurationRange> ranges = ReconfigurationPlan.findAllReconfigurationRanges(table_name, key, 
                                    this.incoming_ranges_map, catalogContext, enclosing_range);
                            for (ReconfigurationRange range : ranges) {
                                LOG.info(String.format("Access for key %s, pulling entire range :%s (%s)", key, range.toString(),table_name));
                                rangesToPull.add(range);
                            }
                        }
                        else {
                            Set<ReconfigurationRange> ranges;
                            for(String rTableName : relatedTables){
                                ranges = ReconfigurationPlan.findAllReconfigurationRanges(rTableName, key, 
                                        this.incoming_ranges_map, catalogContext, enclosing_range);
                                for (ReconfigurationRange range : ranges) {
                                    boolean already_pulled = true;
                                    for (int i = 0; i < range.getMinIncl().size() && i < range.getMaxExcl().size(); ++i) {
                                        // Every ReconfigurationRange in dataMigratedIn contains exactly one contiguous range 
                                        // (i.e. one min and one max value) so that findReconfigurationRangeConcurrent will be correct
                                        ReconfigurationRange new_range = new ReconfigurationRange(range.getTableName(), range.getKeySchema(), 
                                                range.getMinIncl().get(i), range.getMaxExcl().get(i), range.getOldPartition(), range.getNewPartition());
                                        if (!dataMigratedIn.containsKey(new_range)) {
                                            LOG.info(String.format("Access for key %s, pulling entire range :%s (%s)", key, range.toString(),rTableName));
                                            rangesToPull.add(range);
                                            already_pulled = false;
                                            break;
                                        }
                                    }
                                    if (already_pulled){
                                        LOG.info(String.format("Range %s has already been migrated in. Not pulling again", range));
                                    } 
                                }
                            }
                        }                           
                    } catch (Exception e) {
                        LOG.error("Exception checkKeyOwned",e);
                        throw new RuntimeException(e);
                    }
                    ReconfigurationException ex = new ReconfigurationException(ExceptionTypes.TUPLES_NOT_MIGRATED, previousPartition,expectedPartition,rangesToPull);
                    throw ex;
                }
            }

        } else if (expectedPartition != partition_id &&  previousPartition == partition_id) {
            //Key should be moving away
            if (debug.val) LOG.debug(String.format("Key %s was at %s. Checking if migrated ",key,partition_id));

            //Check to see if we migrated this key individually
            if (checkMigratedMapSet(migratedKeyOut,table_name,key)){
                ReconfigurationException ex = new ReconfigurationException(ExceptionTypes.TUPLES_MIGRATED_OUT,table, previousPartition,expectedPartition, key);
                throw ex;
            }



            //check to see if this key was migrated in a range
            ReconfigurationRange range = ReconfigurationPlan.findReconfigurationRangeConcurrent(table_name, key, this.dataMigratedOut, catalogContext);
            if(range != null){
                ReconfigurationException ex = new ReconfigurationException(ExceptionTypes.TUPLES_MIGRATED_OUT,table, previousPartition,expectedPartition, key);
                throw ex;
            }

            //check to see if this key was migrated in a range
            range = ReconfigurationPlan.findReconfigurationRangeConcurrent(table_name, key, this.dataPartiallyMigratedOut, catalogContext);
            if(range != null){
                ReconfigurationException ex = new ReconfigurationException(ExceptionTypes.TUPLES_MIGRATED_OUT,table, previousPartition,expectedPartition, key);
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

    @Override
    public boolean checkIfAllRangesAreMigratedIn() {       
        if(incomingRangesCount == rangesMigratedInCount){
            return true;
        } 
        return false;     
    }

    @Override
    public Set<Integer> getAllPartitionIds(List<CatalogType> catalog, List<Object> key) throws Exception {

        String table_name = this.partitionPlan.getTableName(catalog);
        Set<Integer> partitionIds = new HashSet<Integer>();
        partitionIds.addAll(this.partitionPlan.getAllPartitionIds(table_name, key));
        partitionIds.addAll(this.partitionPlan.getAllPreviousPartitionIds(table_name, key));
        return partitionIds;
    }


}
