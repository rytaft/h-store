/**
 * 
 */
package edu.brown.hashing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;

import edu.brown.designer.MemoryEstimator;
import edu.brown.hashing.PlannedPartitions.PartitionKeyComparator;
import edu.brown.hashing.PlannedPartitions.PartitionPhase;
import edu.brown.hashing.PlannedPartitions.PartitionRange;
import edu.brown.hashing.PlannedPartitions.PartitionedTable;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.reconfiguration.ReconfigurationUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * The delta between two partition plans
 * 
 * @author aelmore
 */
public class ReconfigurationPlan {

    private static final Logger LOG = Logger.getLogger(ReconfigurationPlan.class);
    private static final LoggerBoolean debug = new LoggerBoolean(); 
    private static final LoggerBoolean trace = new LoggerBoolean();
    
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    protected Map<String, ReconfigurationTable> tables_map;

    // Helper map of partition ID and outgoing/incoming ranges for this
    // reconfiguration
    protected Map<Integer, Map<String, TreeSet<ReconfigurationRange>>> outgoing_ranges_map;
    protected Map<Integer, Map<String, TreeSet<ReconfigurationRange>>> incoming_ranges_map;
    protected Map<Integer, List<ReconfigurationRange>> outgoing_ranges;
    protected Map<Integer, List<ReconfigurationRange>> incoming_ranges;
    protected Map<String, TreeSet<ReconfigurationRange>> range_map;
    protected Map<ReconfigurationRange, ReconfigurationRange> enclosing_range;
    public String planDebug = "";
    private CatalogContext catalogContext;
    protected Map<String, String> partitionedTablesByFK;
    protected Map<String, List<String>> relatedTablesMap;
    protected PartitionKeyComparator cmp;
    
    private void getRelatedTables() {
        // find reverse map of fk partitioning
        relatedTablesMap = new HashMap<>();
        for (Entry<String, String> entry : partitionedTablesByFK.entrySet()) {
            if (!relatedTablesMap.containsKey(entry.getValue())) {
                relatedTablesMap.put(entry.getValue(), new ArrayList<String>());
            }
            relatedTablesMap.get(entry.getValue()).add(entry.getKey());
        }
    }
    
    public ReconfigurationPlan(CatalogContext catalogContext, Map<String, String> partitionedTablesByFK) {
        this.catalogContext = catalogContext;
        outgoing_ranges = new HashMap<>();
        incoming_ranges = new HashMap<>();
        outgoing_ranges_map = new HashMap<>();
        incoming_ranges_map = new HashMap<>();
        range_map = new HashMap<>();
        enclosing_range = new HashMap<>();
        tables_map = new HashMap<>();
        this.partitionedTablesByFK = partitionedTablesByFK;
        getRelatedTables();
        this.cmp = new PartitionKeyComparator();
    }

    public void addRange(ReconfigurationRange range) {

        if (!outgoing_ranges.containsKey(range.old_partition)) {
            outgoing_ranges.put(range.old_partition, new ArrayList<ReconfigurationRange>());
        }
        if (!incoming_ranges.containsKey(range.new_partition)) {
            incoming_ranges.put(range.new_partition, new ArrayList<ReconfigurationRange>());
        }
        if (!outgoing_ranges_map.containsKey(range.old_partition)) {
            outgoing_ranges_map.put(range.old_partition, new HashMap<String, TreeSet<ReconfigurationRange>>());
        }
        if (!outgoing_ranges_map.get(range.old_partition).containsKey(range.table_name)) {
            outgoing_ranges_map.get(range.old_partition).put(range.table_name, new TreeSet<ReconfigurationRange>());
        }
        if (!incoming_ranges_map.containsKey(range.new_partition)) {
            incoming_ranges_map.put(range.new_partition, new HashMap<String, TreeSet<ReconfigurationRange>>());
        }
        if (!incoming_ranges_map.get(range.new_partition).containsKey(range.table_name)) {
            incoming_ranges_map.get(range.new_partition).put(range.table_name, new TreeSet<ReconfigurationRange>());
        }
        if (!range_map.containsKey(range.table_name)) {
            range_map.put(range.table_name, new TreeSet<ReconfigurationRange>());
        }
        outgoing_ranges.get(range.old_partition).add(range);
        incoming_ranges.get(range.new_partition).add(range);
        for (int i = 0; i < range.getMinIncl().size() && i < range.getMaxExcl().size(); ++i) {
            // Every ReconfigurationRange in range_map should contain exactly one contiguous range (i.e. one min and one max value)
            // so that findReconfigurationRange and findAllReconfigurationRanges will be correct
            ReconfigurationRange new_range = new ReconfigurationRange(range.table_name, range.keySchema, 
                    range.min_incl.get(i), range.max_excl.get(i), range.old_partition, range.new_partition);
            range_map.get(range.table_name).add(new_range);
            outgoing_ranges_map.get(range.old_partition).get(range.table_name).add(new_range);
            incoming_ranges_map.get(range.new_partition).get(range.table_name).add(new_range);
            enclosing_range.put(new_range, range);
        }
    }

    /**
     * @throws Exception
     */
    public ReconfigurationPlan(CatalogContext catalogContext, PartitionPhase old_phase, PartitionPhase new_phase) throws Exception {
        this.catalogContext = catalogContext;
        outgoing_ranges = new HashMap<>();
        incoming_ranges = new HashMap<>();
        outgoing_ranges_map = new HashMap<>();
        incoming_ranges_map = new HashMap<>();
        range_map = new HashMap<>();
        enclosing_range = new HashMap<>();
        partitionedTablesByFK = old_phase.partitionedTablesByFK;
        getRelatedTables();
        this.cmp = new PartitionKeyComparator();
        assert old_phase.tables_map.keySet().equals(new_phase.tables_map.keySet()) : "Partition plans have different tables";
        tables_map = new HashMap<String, ReconfigurationPlan.ReconfigurationTable>();
        for (String table_name : old_phase.tables_map.keySet()) {
            if (!partitionedTablesByFK.containsKey(table_name)) {
                tables_map.put(table_name, new ReconfigurationTable(catalogContext, old_phase.getTable(table_name), new_phase.getTable(table_name), relatedTablesMap.get(table_name)));
            }
        }
        // add ReconfigurationTables for the tables partitioned by FK second so they get the same range splits as their parents
        for (Map.Entry<String,String> entry : partitionedTablesByFK.entrySet()) {
            String table_name = entry.getKey();
            ReconfigurationTable parent_table = tables_map.get(entry.getValue());
            tables_map.put(table_name, new ReconfigurationTable(table_name, parent_table));
        }
        registerReconfigurationRanges();
        if (debug.val) {
            planDebug = String.format("Reconfiguration plan generated \n Out: %s \n In: %s", outgoing_ranges.toString(), incoming_ranges.toString());
            LOG.debug(planDebug);
            LOG.debug(String.format("Range maps: \n Out: %s \n In: %s", outgoing_ranges_map.toString(), incoming_ranges_map.toString()));
            LOG.debug(String.format("Enclosing range map: %s", enclosing_range.toString()));
        }
    }

    protected void registerReconfigurationRanges() {
        for (String table_name : tables_map.keySet()) {
            for (ReconfigurationRange range : tables_map.get(table_name).getReconfigurations()) {
                addRange(range);
            }
        }
    }

    /**
     * Find the reconfiguration range for a key
     * 
     * @param id
     * @return the reconfiguration range or null if no match could be found
     */
    public ReconfigurationRange findReconfigurationRange(String table_name, List<Object> ids) throws Exception {
        return findReconfigurationRange(table_name, ids, this.range_map, this.catalogContext, this.enclosing_range);
    }
    
    /**
     * Find the reconfiguration range for a key
     * 
     * @return the reconfiguration range or null if no match could be found
     */
    public static ReconfigurationRange findReconfigurationRangeConcurrent(String table_name, List<Object> ids, 
            ConcurrentSkipListMap<ReconfigurationRange,String> range_map, CatalogContext catalogContext) {
        try {
            if(range_map == null) {
                return null;
            }
            
            Object[] keys = ids.toArray();
            ReconfigurationRange range = new ReconfigurationRange(catalogContext.getTableByName(table_name), keys, keys, 0, 0);

            ReconfigurationRange precedingRange = range_map.floorKey(range);
            if (precedingRange != null && precedingRange.table_name.equalsIgnoreCase(table_name) && precedingRange.inRange(keys)) {
                return precedingRange;
            }

            ReconfigurationRange followingRange = range_map.ceilingKey(range);
            if (followingRange != null && followingRange.table_name.equalsIgnoreCase(table_name) && followingRange.inRange(keys)) {
                return followingRange;
            }

        } catch (Exception e) {
            LOG.error(String.format("Error looking up reconfiguration range. Table:%s Ids:%s", table_name, ids), e);
        }

        return null;
    }

    
    /**
     * Find the reconfiguration range for a key
     * 
     * @return the reconfiguration range or null if no match could be found
     */
    public static ReconfigurationRange findReconfigurationRange(String table_name, List<Object> ids, 
            Map<String, TreeSet<ReconfigurationRange>> range_map, CatalogContext catalogContext,
            Map<ReconfigurationRange, ReconfigurationRange> enclosing_range) throws Exception {
        try {
            if(range_map == null) {
                return null;
            }
            
            TreeSet<ReconfigurationRange> ranges = range_map.get(table_name);
            if (ranges == null) {
                return null;
            }
            
            Object[] keys = ids.toArray();
            ReconfigurationRange range = new ReconfigurationRange(catalogContext.getTableByName(table_name), keys, keys, 0, 0);

            ReconfigurationRange precedingRange = ranges.floor(range);
            if (precedingRange != null && precedingRange.inRange(keys)) {
                if(enclosing_range.containsKey(precedingRange)) {
                    return enclosing_range.get(precedingRange);
                }
                return precedingRange;
            }

            ReconfigurationRange followingRange = ranges.ceiling(range);
            if (followingRange != null && followingRange.inRange(keys)) {
                if(enclosing_range.containsKey(followingRange)) {
                    return enclosing_range.get(followingRange);
                }
                return followingRange;
            }

        } catch (Exception e) {
            LOG.error(String.format("Error looking up reconfiguration range. Table:%s Ids:%s", table_name, ids), e);
        }

        return null;
    }

    /**
     * Find all reconfiguration ranges that may contain a key
     * 
     * @param id
     * @return the matching reconfiguration ranges
     */
    public Set<ReconfigurationRange> findAllReconfigurationRanges(String table_name, List<Object> ids) throws Exception {
        return findAllReconfigurationRanges(table_name, ids, this.range_map, this.catalogContext, this.enclosing_range);
    }

    /**
     * Find all reconfiguration ranges that may contain a key
     * 
     * @return the matching reconfiguration ranges
     */
    public static Set<ReconfigurationRange> findAllReconfigurationRanges(String table_name, List<Object> ids,
            Map<String, TreeSet<ReconfigurationRange>> range_map, CatalogContext catalogContext,
            Map<ReconfigurationRange, ReconfigurationRange> enclosing_range) throws Exception {
        Set<ReconfigurationRange> matchingRanges = new HashSet<ReconfigurationRange>();
        if(range_map == null) {
            return matchingRanges;
        }
        
        TreeSet<ReconfigurationRange> ranges = range_map.get(table_name);
        if (ranges == null) {
            return matchingRanges;
        }
        
        Object[] keys = ids.toArray();
        ReconfigurationRange range = new ReconfigurationRange(catalogContext.getTableByName(table_name), keys, keys, 0, 0);
        ReconfigurationRange precedingRange = ranges.floor(range);
        if (precedingRange != null && precedingRange.overlapsRange(keys)) {
            matchingRanges.add(precedingRange);
        }
        for (ReconfigurationRange r : ranges.tailSet(range, false)) {
            try {
                if (r.overlapsRange(keys)) {
                    if(enclosing_range.containsKey(r)) {
                        matchingRanges.add(enclosing_range.get(r));
                    } else {
                        matchingRanges.add(r);
                    }
                }
                else {
                    break;
                }
            } catch (Exception e) {
                LOG.error("Error looking up reconfiguration range", e);
            }
        }

        return matchingRanges;
    }

    public static class ReconfigurationTable {
        private List<ReconfigurationRange> reconfigurations;
        String table_name;
        HStoreConf conf = null;
        CatalogContext catalogContext;
        
        public ReconfigurationTable(String table_name, ReconfigurationTable parent) {
            this.catalogContext = parent.catalogContext;
            this.table_name = table_name;
            this.conf = HStoreConf.singleton(false);
            setReconfigurations(new ArrayList<ReconfigurationRange>());
            for (ReconfigurationRange range : parent.getReconfigurations()) {
                getReconfigurations().add(range.clone(catalogContext.getTableByName(table_name)));
            }
        }
        
        public ReconfigurationTable(CatalogContext catalogContext, PartitionedTable old_table, PartitionedTable new_table) throws Exception {
            this(catalogContext, old_table, new_table, null);
        }
         
        public ReconfigurationTable(CatalogContext catalogContext, PartitionedTable old_table, PartitionedTable new_table, List<String> relatedTables) throws Exception {
            this.catalogContext = catalogContext;
            table_name = old_table.table_name;
            this.conf = HStoreConf.singleton(false);
            setReconfigurations(new ArrayList<ReconfigurationRange>());
            Iterator<PartitionRange> old_ranges = old_table.getRanges().iterator();
            Iterator<PartitionRange> new_ranges = new_table.getRanges().iterator();

            PartitionRange new_range = new_ranges.next();
            PartitionKeyComparator cmp = new PartitionKeyComparator();
            
            Object[] max_old_accounted_for = null;

            PartitionRange old_range = null;
            // Iterate through the old partition ranges.
            // Only move to the next old rang
            while (old_ranges.hasNext() || (max_old_accounted_for != null && (cmp.compare(max_old_accounted_for, old_range.getMaxExcl())) != 0)) {
                // only move to the next element if first time, or all of the
                // previous
                // range has been accounted for
                if (old_range == null || cmp.compare(old_range.getMaxExcl(), max_old_accounted_for) <= 0) {
                    old_range = old_ranges.next();
                }

                if (max_old_accounted_for == null) {
                    // We have not accounted for any range yet
                    max_old_accounted_for = old_range.getMinIncl();
                }
                if (old_range.compareTo(new_range) == 0) {
                    if (old_range.getPartition() == new_range.getPartition()) {
                        // No change do nothing
                    } else {
                        // Same range new partition
                        getReconfigurations()
                                .add(new ReconfigurationRange(this.table_name, old_range.getKeySchema(), old_range.getMinIncl(), old_range.getMaxExcl(), old_range.getPartition(), new_range
                                        .getPartition()));
                    }
                    max_old_accounted_for = old_range.getMaxExcl();
                    if (new_ranges.hasNext())
                        new_range = new_ranges.next();
                } else {
                    if (cmp.compare(old_range.getMaxExcl(), new_range.getMaxExcl()) <= 0) {
                        // The old range is a subset of the new range
                        if (old_range.getPartition() == new_range.getPartition()) {
                            // Same partitions no reconfiguration needed here
                            max_old_accounted_for = old_range.getMaxExcl();
                        } else {
                            // Need to move the old range to new range
                            getReconfigurations().add(
                                    new ReconfigurationRange(this.table_name, old_range.getKeySchema(), max_old_accounted_for, old_range.getMaxExcl(), old_range.getPartition(), new_range
                                            .getPartition()));
                            max_old_accounted_for = old_range.getMaxExcl();
                        }
                        // Have we satisfied all of the new range and is there
                        // another new range to process
                        if (cmp.compare(max_old_accounted_for, new_range.getMaxExcl()) == 0 && new_ranges.hasNext()) {
                            new_range = new_ranges.next();
                        }

                    } else {
                        // The old range is larger than this new range
                        // keep getting new ranges until old range has been
                        // satisfied
                        while (cmp.compare(old_range.getMaxExcl(), new_range.getMaxExcl()) > 0) {
                            if (old_range.getPartition() == new_range.getPartition()) {
                                // No need to move this range
                                max_old_accounted_for = new_range.getMaxExcl();
                            } else {
                                // move
                                getReconfigurations().add(
                                        new ReconfigurationRange(this.table_name, new_range.getKeySchema(), max_old_accounted_for, new_range.getMaxExcl(), old_range.getPartition(), new_range
                                                .getPartition()));
                                max_old_accounted_for = new_range.getMaxExcl();
                            }
                            if (new_ranges.hasNext() == false) {
                                throw new RuntimeException("Not all ranges accounted for");
                            }
                            new_range = new_ranges.next();
                        }
                    }

                }
            }
            if (!this.catalogContext.jarPath.getName().contains("tpcc")) {
                setReconfigurations(mergeReconfigurations(splitReconfigurations(getReconfigurations(), new_table.getCatalog_table(), relatedTables), new_table.getCatalog_table()));
            } else {
                LOG.info("skipping merging");
            }

        }

        private List<ReconfigurationRange> mergeReconfigurations(List<ReconfigurationRange> reconfiguration_range, Table catalog_table) {
            if (catalog_table == null) {
                LOG.info("Catalog table is null. Not merging reconfigurations");
                return reconfiguration_range;
            }
            List<ReconfigurationRange> res = new ArrayList<>();

            // Check if we should merge
            if (conf == null)
                return reconfiguration_range;
            long currentMin = conf.site.reconfig_min_transfer_bytes;
            if (currentMin <= 1) {
                LOG.info(String.format("Not merging reconfiguration plan. Min transfer bytes: %s", conf.site.reconfig_min_transfer_bytes));
                return reconfiguration_range;
            }

            try {

                long tupleBytes = MemoryEstimator.estimateTupleSize(catalog_table);
                long minRows = currentMin / tupleBytes;
                LOG.info(String.format("Trying to merge on table:%s  TupleBytes:%s  CurrentMin:%s  MinRows:%s MinTransferBytes:%s", catalog_table.fullName(), tupleBytes, currentMin, minRows,
                        currentMin));

                HashMap<String, ReconfigurationRange> rangeMap = new HashMap<>();
                for (ReconfigurationRange range : reconfiguration_range) {

                    // only merge ranges that have the same old partition and
                    // same new partition
                    int old_partition = range.old_partition;
                    int new_partition = range.new_partition;
                    String key = new String(old_partition + "->" + new_partition);
                    ReconfigurationRange partialRange = rangeMap.get(key);
                    if (partialRange == null) {
                        VoltTable newMin = range.getKeySchema().clone(0);
                        VoltTable newMax = range.getKeySchema().clone(0);
                        partialRange = new ReconfigurationRange(this.table_name, range.getKeySchema(), newMin, newMax, old_partition, new_partition);
                        rangeMap.put(key, partialRange);
                    }

                    Object[] max = range.getMaxExcl().get(0);
                    Object[] min = range.getMinIncl().get(0);
                    partialRange.getMaxExcl().add(max);
                    partialRange.getMinIncl().add(min);
                    partialRange.truncateNullCols();
                    partialRange.updateMinMax();
                    long max_potential_keys = partialRange.getMaxPotentialKeys();

                    // once we have reached the minimum number of rows, we can
                    // add this set of ranges to the output
                    if (max_potential_keys >= minRows) {
                        int num_ranges = partialRange.getMaxExcl().size();
                        if (num_ranges > 1) {
                            LOG.info(String.format("Merging %s ranges. Table:%s", num_ranges, table_name));
                        }

                        res.add(partialRange);
                        partialRange = null;
                        rangeMap.remove(key);
                    }

                }

                // and don't forget to add the remaining sets of ranges that
                // didn't reach the minimum number of rows
                for (Map.Entry<String, ReconfigurationRange> rangeEntry : rangeMap.entrySet()) {
                    int num_ranges = rangeEntry.getValue().getMaxExcl().size();
                    if (num_ranges > 1) {
                        LOG.info(String.format("Merging %s ranges. Table:%s", num_ranges, table_name));
                    }

                    res.add(rangeEntry.getValue());
                }

            } catch (Exception ex) {
                LOG.error("Exception merging reconfiguration ranges, returning original list", ex);
                return reconfiguration_range;
            }

            return res;
        }

        private double getTuplesPerKey(String name) {
            double tuples_per_key = 1;
            
            ////// HACK for B2W ///////           
//            if (name.equalsIgnoreCase("CART")) tuples_per_key = 1;
//            else if (name.equalsIgnoreCase("CART_CUSTOMER")) tuples_per_key = 1;
//            else if (name.equalsIgnoreCase("CART_LINE_PRODUCTS")) tuples_per_key = 1;
//            else if (name.equalsIgnoreCase("CART_LINE_PRODUCT_STORES")) tuples_per_key = 1;
//            else if (name.equalsIgnoreCase("CART_LINE_PRODUCT_WARRANTIES")) tuples_per_key = 1;
//            else if (name.equalsIgnoreCase("CART_LINE_PROMOTIONS")) tuples_per_key = 1;
//            else if (name.equalsIgnoreCase("CART_LINES")) tuples_per_key = 1;
//            else if (name.equalsIgnoreCase("CHECKOUT")) tuples_per_key = 1;
//            else if (name.equalsIgnoreCase("CHECKOUT_FREIGHT_DELIVERY_TIME")) tuples_per_key = 1;
//            else if (name.equalsIgnoreCase("CHECKOUT_PAYMENTS")) tuples_per_key = 1;
//            else if (name.equalsIgnoreCase("CHECKOUT_STOCK_TRANSACTIONS")) tuples_per_key = 1;
            if (name.equalsIgnoreCase("STK_INVENTORY_STOCK")) tuples_per_key = 0;
            else if (name.equalsIgnoreCase("STK_INVENTORY_STOCK_QUANTITY")) tuples_per_key = 0;
            else if (name.equalsIgnoreCase("STK_STOCK_TRANSACTION")) tuples_per_key = 0;
            return tuples_per_key;
        }
        
        private List<ReconfigurationRange> splitReconfigurations(List<ReconfigurationRange> reconfiguration_range, Table catalog_table, List<String> relatedTables) {
            if (catalog_table == null) {
                LOG.info("Catalog table is null. Not splitting reconfigurations");
                return reconfiguration_range;
            }

            // Check if we should split
            if (conf == null)
                return reconfiguration_range;
            long currentMax = conf.site.reconfig_max_transfer_bytes;
            if (currentMax <= 1)
                return reconfiguration_range;

            boolean modified = false;
            try {
                
                double tuples_per_key = getTuplesPerKey(catalog_table.getName());
                double tupleBytes = MemoryEstimator.estimateTupleSize(catalog_table) * tuples_per_key;
                if (relatedTables != null) {
                    for (String relatedTable : relatedTables) {
                        tuples_per_key = getTuplesPerKey(relatedTable);
                        Table relatedCatalogTable = this.catalogContext.getTableByName(relatedTable);
                        tupleBytes += MemoryEstimator.estimateTupleSize(relatedCatalogTable) * tuples_per_key;
                    }
                }

                long maxRows = currentMax;
                if (tupleBytes != 0) {
                    maxRows = (long) (currentMax / tupleBytes);
                } 
                LOG.info(String.format("Trying to split on table:%s  TupleBytes:%s  CurrentMax:%s  MaxRows:%s MaxTransferBytes:%s", catalog_table.fullName(), tupleBytes, currentMax, maxRows,
                        currentMax));

                List<ReconfigurationRange> res = new ArrayList<>();
                for (ReconfigurationRange range : reconfiguration_range) {
                    long max_potential_keys = range.getMaxPotentialKeys();
                    if (max_potential_keys > maxRows) {
                        long orig_max = ((Number) range.getMaxExcl().get(0)[0]).longValue();
                        long orig_min = ((Number) range.getMinIncl().get(0)[0]).longValue();
                        LOG.info(String.format("Splitting up a range %s-%s. Max row:%s. Table:%s", orig_min, orig_max, maxRows, table_name));
                        long new_max, new_min;
                        new_min = orig_min;
                        long keysRemaining = max_potential_keys;

                        // We need to split up this range
                        while (keysRemaining > 0) {
                            new_max = Math.min(new_min + maxRows, orig_max);
                            LOG.debug(String.format("New range %s-%s", new_min, new_max));
                            VoltTable max, min;

                            if (new_max == orig_max) {
                                max = range.getMaxExclTable();
                            } else {
                                max = range.getKeySchema().clone(0);
                                Object[] values = new Object[max.getColumnCount()];
                                values[0] = new_max;
                                for (int i = 1; i < max.getColumnCount(); i++) {
                                    VoltType vt = max.getColumnType(i);
                                    values[i] = vt.getNullValue();
                                }
                                max.addRow(values);
                            }

                            if (new_min == orig_min) {
                                min = range.getMinInclTable();
                            } else {
                                min = range.getKeySchema().clone(0);
                                Object[] values = new Object[min.getColumnCount()];
                                values[0] = new_min;
                                for (int i = 1; i < min.getColumnCount(); i++) {
                                    VoltType vt = min.getColumnType(i);
                                    values[i] = vt.getNullValue();
                                }
                                min.addRow(values);
                            }

                            ReconfigurationRange newRange = new ReconfigurationRange(this.table_name, range.getKeySchema(), min, max, range.old_partition, range.new_partition);
                            new_min = new_max;
                            keysRemaining -= maxRows;
                            modified = true;
                            res.add(newRange);
                        }

                    } else {
                        // This range is ok to keep
                        res.add(range);
                    }

                }

                if (modified) {
                    return res;
                }
            } catch (Exception ex) {
                LOG.error("Exception splitting reconfiguration ranges, returning original list", ex);
            }

            return reconfiguration_range;
        }

        public List<ReconfigurationRange> getReconfigurations() {
            return reconfigurations;
        }

        public void setReconfigurations(List<ReconfigurationRange> reconfigurations) {
            this.reconfigurations = reconfigurations;
        }
    }

    /**
     * A partition range that holds old and new partition IDs. As of 2.4.14 a
     * range may not be non-contiguous, so a range may actually hold a set of
     * ranges. A range is the granual of migration / reconfiguration. As of
     * 5/20/14 a range may also contain a sub-range on a different key in the
     * case of multi-column partitioning
     * 
     * @author aelmore, rytaft
     */
    public static class ReconfigurationRange implements Comparable<ReconfigurationRange> {
        private int old_partition;
        private int new_partition;
        private String table_name;
        private VoltTable keySchema;
        private List<Object[]> min_incl;
        private List<Object[]> max_excl;
        private PartitionKeyComparator cmp;
        private Table catalog_table;
        private Object[] smallest_min_incl;
        private Object[] largest_max_excl;

        public ReconfigurationRange(String table_name, VoltTable keySchema, VoltTable min_incl, VoltTable max_excl, int old_partition, int new_partition) {
            this(table_name, keySchema, new ArrayList<Object[]>(), new ArrayList<Object[]>(), old_partition, new_partition);

            min_incl.resetRowPosition();
            max_excl.resetRowPosition();
            while (min_incl.advanceRow() && max_excl.advanceRow()) {
                this.min_incl.add(min_incl.getRowArray());
                this.max_excl.add(max_excl.getRowArray());
            }

            this.truncateNullCols();
            this.updateMinMax();
        }

        public ReconfigurationRange(Table table, VoltTable min_incl, VoltTable max_excl, int old_partition, int new_partition) {
            this(table, new ArrayList<Object[]>(), new ArrayList<Object[]>(), old_partition, new_partition);

            min_incl.resetRowPosition();
            max_excl.resetRowPosition();
            while (min_incl.advanceRow() && max_excl.advanceRow()) {
                this.min_incl.add(min_incl.getRowArray());
                this.max_excl.add(max_excl.getRowArray());
            }

            this.truncateNullCols();
            this.updateMinMax();
        }

        public ReconfigurationRange(String table_name, VoltTable keySchema, Object[] min_incl, Object[] max_excl, int old_partition, int new_partition) {
            this(table_name, keySchema, new ArrayList<Object[]>(), new ArrayList<Object[]>(), old_partition, new_partition);

            this.min_incl.add(min_incl);
            this.max_excl.add(max_excl);

            this.truncateNullCols();
            this.updateMinMax();
        }

        public ReconfigurationRange(Table table, Object[] min_incl, Object[] max_excl, int old_partition, int new_partition) {
            this(table, new ArrayList<Object[]>(), new ArrayList<Object[]>(), old_partition, new_partition);

            this.min_incl.add(min_incl);
            this.max_excl.add(max_excl);

            this.truncateNullCols();
            this.updateMinMax();
        }
        
        public ReconfigurationRange(String table_name, VoltTable keySchema, List<Object[]> min_incl, List<Object[]> max_excl, int old_partition, int new_partition) {
            this.keySchema = keySchema;

            this.cmp = new PartitionKeyComparator();

            this.min_incl = min_incl;
            this.max_excl = max_excl;

            this.old_partition = old_partition;
            this.new_partition = new_partition;
            this.table_name = table_name;

            this.truncateNullCols();
            this.updateMinMax();
        }

        public ReconfigurationRange(Table table, List<Object[]> min_incl, List<Object[]> max_excl, int old_partition, int new_partition) {
            this.catalog_table = table;
            this.keySchema = ReconfigurationUtil.getPartitionKeysVoltTable(table);

            this.cmp = new PartitionKeyComparator();

            this.min_incl = min_incl;
            this.max_excl = max_excl;

            this.old_partition = old_partition;
            this.new_partition = new_partition;
            this.table_name = table.getName().toLowerCase();

            this.truncateNullCols();
            this.updateMinMax();
        }

        public ReconfigurationRange clone(Table new_table) {
            List<Object[]> minInclClone = new ArrayList<Object[]>();
            List<Object[]> maxExclClone = new ArrayList<Object[]>();
            minInclClone.addAll(this.min_incl);
            maxExclClone.addAll(this.max_excl);
            ReconfigurationRange clone = new ReconfigurationRange(this.table_name, this.keySchema.clone(0), minInclClone, maxExclClone, this.old_partition, this.new_partition);
            clone.table_name = new_table.getName().toLowerCase();
            clone.catalog_table = new_table;
            return clone;
        }

        @Override
        public int compareTo(ReconfigurationRange o) {
            if (table_name != null && !table_name.equals(o.table_name)) {
                return table_name.compareTo(o.table_name);
            }
            
            int min_incl_cmp = cmp.compare(this.smallest_min_incl, o.smallest_min_incl);
            if (min_incl_cmp < 0) {
                return -1;
            } else if (min_incl_cmp == 0) {
                return cmp.compare(this.largest_max_excl, o.largest_max_excl);
            } else {
                return 1;
            }
        }

        @Override
        public String toString() {
            String keys = "";
            int row = 0;

            for (int i = 0; i < this.min_incl.size() && i < this.max_excl.size(); i++) {
                Object[] min_incl_i = this.min_incl.get(i);
                Object[] max_excl_i = this.max_excl.get(i);

                if (row != 0) {
                    keys += ", ";
                }

                String min_str = "";
                String max_str = "";
                for (int j = 0; j < min_incl_i.length; j++) {
                    if (j != 0) {
                        min_str += ":";
                    }
                    min_str += min_incl_i[j].toString();
                }
                for (int j = 0; j < max_excl_i.length; j++) {
                    if (j != 0) {
                        max_str += ":";
                    }
                    max_str += max_excl_i[j].toString();
                }
                keys += "[" + min_str + "-" + max_str + ")";
                row++;
            }
            return String.format("ReconfigRange (%s) keys:%s p_id:%s->%s ", table_name, keys, old_partition, new_partition);

        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + new_partition;
            result = prime * result + old_partition;
            result = prime * result + min_incl.hashCode();
            result = prime * result + max_excl.hashCode();
            result = prime * result + ((table_name == null) ? 0 : table_name.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }

            ReconfigurationRange other = (ReconfigurationRange) obj;
            if (new_partition != other.new_partition) {
                return false;
            }
            if (old_partition != other.old_partition) {
                return false;
            }

            if (table_name == null) {
                if (other.table_name != null) {
                    return false;
                }
            } else if (!table_name.equals(other.table_name)) {
                return false;
            }

            if (min_incl == null) {
                if (other.min_incl != null) {
                    return false;
                }
            } else {
                if (min_incl.size() != other.min_incl.size()) {
                    return false;
                }
                for (int i = 0; i < min_incl.size(); i++) {
                    if (!Arrays.asList(min_incl.get(i)).equals(Arrays.asList(other.min_incl.get(i)))) {
                        return false;
                    }
                }
            }

            if (max_excl == null) {
                if (other.max_excl != null) {
                    return false;
                }
            } else {
                if (max_excl.size() != other.max_excl.size()) {
                    return false;
                }
                for (int i = 0; i < max_excl.size(); i++) {
                    if (!Arrays.asList(max_excl.get(i)).equals(Arrays.asList(other.max_excl.get(i)))) {
                        return false;
                    }
                }
            }
            return true;
        }

        public boolean inRange(Object[] keys) {
            for (int i = 0; i < this.min_incl.size() && i < this.max_excl.size(); i++) {
                Object[] min_incl_i = this.min_incl.get(i);
                Object[] max_excl_i = this.max_excl.get(i);
                int min_incl_cmp = cmp.compare(min_incl_i, keys);
                // the following ensures that [6] is not in the ranges [5,6:2),
                // [6,6:2), [6:2,6:2) or [6:2,7)
                // but is in the ranges [6,7:2), [5,7), [6,7), [6,6), etc.
                if (min_incl_cmp <= 0 && (cmp.compare(max_excl_i, keys) == 1 || (cmp.compare(min_incl_i, max_excl_i) == 0 && min_incl_cmp == 0))) {
                    return true;
                }
            }
            return false;
        }

        public boolean overlapsRange(Object[] keys) {
            for (int i = 0; i < this.min_incl.size() && i < this.max_excl.size(); i++) {
                Object[] min_incl_i = this.min_incl.get(i);
                Object[] max_excl_i = this.max_excl.get(i);
                int min_incl_cmp = cmp.compare(min_incl_i, keys);
                // the following ensures that [6] overlaps the ranges [5,6:2),
                // [6,6:2), [6:2,6:2), [6:2,7), etc.
                // as well as all the ranges that satisfy inRange(),
                // but not [5,6), [7,8), etc.
                if ((min_incl_cmp <= 0 || min_incl_cmp == 2) && (cmp.compare(max_excl_i, keys) > 0 || (cmp.compare(min_incl_i, max_excl_i) == 0 && (min_incl_cmp == 0 || min_incl_cmp == 2)))) {
                    return true;
                }
            }
            return false;
        }

        public static Object[] getKeys(List<Object> ids, Table table) {
            VoltTable keySchema = ReconfigurationUtil.getPartitionKeysVoltTable(table);
            Object[] keys = new Object[keySchema.getColumnCount()];
            int col = 0;
            for (Object id : ids) {
                if (col >= keys.length) {
                    break;
                }
                keys[col] = id;
                col++;
            }
            for (; col < keySchema.getColumnCount(); col++) {
                VoltType vt = keySchema.getColumnType(col);
                keys[col] = vt.getNullValue();
            }

            keySchema.addRow(keys);
            keySchema.advanceToRow(0);
            return keySchema.getRowArray();
        }

        public Long getMaxPotentialKeys() {
            ArrayList<Long> min_list = new ArrayList<>();
            ArrayList<Long> max_list = new ArrayList<>();
            for (int i = 0; i < this.min_incl.size() && i < this.max_excl.size(); i++) {
                min_list.add(((Number) this.min_incl.get(i)[0]).longValue());
                max_list.add(((Number) this.max_excl.get(i)[0]).longValue());
            }

            Long max_potential_keys = 0L;
            for (int i = 0; i < min_list.size() && i < max_list.size(); ++i) {
                max_potential_keys += max_list.get(i) - min_list.get(i);
            }
            return max_potential_keys;
        }

        public int getOldPartition() {
            return this.old_partition;
        }

        public int getNewPartition() {
            return this.new_partition;
        }

        public String getTableName() {
            return this.table_name;
        }

        public VoltTable getMinInclTable() {
            VoltTable minInclTable = this.keySchema.clone(0);
            int column_count = this.keySchema.getColumnCount();
            for (Object[] row : this.min_incl) {
                Object[] padded_row = Arrays.copyOf(row, column_count);
                for (int i = row.length; i < column_count; ++i) {
                    VoltType vt = keySchema.getColumnType(i);
                    padded_row[i] = vt.getNullValue();
                }
                minInclTable.addRow(padded_row);
            }
            return minInclTable;
        }

        public VoltTable getMaxExclTable() {
            VoltTable maxExclTable = this.keySchema.clone(0);
            int column_count = this.keySchema.getColumnCount();
            for (Object[] row : this.max_excl) {
                Object[] padded_row = Arrays.copyOf(row, column_count);
                for (int i = row.length; i < column_count; ++i) {
                    VoltType vt = keySchema.getColumnType(i);
                    padded_row[i] = vt.getNullValue();
                }
                maxExclTable.addRow(padded_row);
            }
            return maxExclTable;
        }

        public List<Object[]> getMinIncl() {
            return this.min_incl;
        }

        public List<Object[]> getMaxExcl() {
            return this.max_excl;
        }

        public VoltTable getKeySchema() {
            return this.keySchema;
        }

        public Table getTable() {
            return this.catalog_table;
        }

        private void truncateNullCols(int row) {
            for (int i = 0; i < min_incl.get(row).length && i < keySchema.getColumnCount(); i++) {
                VoltType vt = keySchema.getColumnType(i);
                if (vt.getNullValue().equals(min_incl.get(row)[i])) {
                    min_incl.set(row, Arrays.copyOf(min_incl.get(row), i));
                    break;
                }
            }
            for (int i = 0; i < max_excl.get(row).length && i < keySchema.getColumnCount(); i++) {
                VoltType vt = keySchema.getColumnType(i);
                if (vt.getNullValue().equals(max_excl.get(row)[i])) {
                    max_excl.set(row, Arrays.copyOf(max_excl.get(row), i));
                    break;
                }
            }
        }

        // Truncates the null columns in each min_inclusive and max_exclusive key.
        // Should be called any time min_incl or max_excl are updated.
        public void truncateNullCols() {
            for (int i = 0; i < this.min_incl.size(); i++) {
                this.truncateNullCols(i);
            }
        }
        
        // Finds the smallest lower bound and largest upper bound of all the ranges
        // in min_incl and max_excl.
        // Should be called any time min_incl or max_excl are updated.
        public void updateMinMax() {
            if (this.min_incl.isEmpty() || this.max_excl.isEmpty()) {
                this.smallest_min_incl = null;
                this.largest_max_excl = null;
            }
            else {
                this.smallest_min_incl = Collections.min(this.min_incl, this.cmp);
                this.largest_max_excl = Collections.max(this.max_excl, this.cmp);
            }
        }

    }

    public Map<Integer, Map<String, TreeSet<ReconfigurationRange>>> getOutgoing_ranges_map() {
        return outgoing_ranges_map;
    }

    public Map<Integer, Map<String, TreeSet<ReconfigurationRange>>> getIncoming_ranges_map() {
        return incoming_ranges_map;
    }
    
    public Map<Integer, List<ReconfigurationRange>> getOutgoing_ranges() {
        return outgoing_ranges;
    }

    public Map<Integer, List<ReconfigurationRange>> getIncoming_ranges() {
        return incoming_ranges;
    }
    
    public Map<ReconfigurationRange, ReconfigurationRange> getEnclosing_range_map() {
        return enclosing_range;
    }

    public CatalogContext getCatalogContext() {
        return catalogContext;
    }

    public Map<String, String> getPartitionedTablesByFK() {
        return this.partitionedTablesByFK;
    }
}
