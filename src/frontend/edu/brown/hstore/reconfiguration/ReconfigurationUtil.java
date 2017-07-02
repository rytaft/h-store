package edu.brown.hstore.reconfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Table;
import org.voltdb.types.SortDirectionType;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTableComparator;
import org.voltdb.VoltType;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hashing.ReconfigurationPlan;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationTable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.CollectionUtil;

public class ReconfigurationUtil {
    private static final Logger LOG = Logger.getLogger(ReconfigurationUtil.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public static class ReconfigurationPair implements Comparable<ReconfigurationPair> {
        public Integer from;
        public Integer to;

        public ReconfigurationPair(Integer from, Integer to) {
            this.from = from;
            this.to = to;
        }

        // order the pairs by the smaller partition first, and then by the larger partition
        @Override
        public int compareTo(ReconfigurationPair other) {
            if (this.from > this.to && other.from > other.to) { // scaling in
                if (this.to < other.to) {
                    return -1;
                } else if (this.to > other.to) {
                    return 1;
                } else if (this.from < other.from) {
                    return -1;
                } else if (this.from > other.from) {
                    return 1;
                } 
            } else { // scaling out
                if (this.from < other.from) {
                    return -1;
                } else if (this.from > other.from) {
                    return 1;
                } else if (this.to < other.to) {
                    return -1;
                } else if (this.to > other.to) {
                    return 1;
                } 
            }
            return 0;
        }

        public String toString() {
            return String.format("(%s->%s)", from, to);
        }

        @Override
        public final int hashCode() {
            return (from == null ? 0 : from.hashCode() * 31) + (to == null ? 0 : to.hashCode());
        }

        /**
         * @param o
         *            Object to compare to.
         * @return Is the object equal to a value in the pair.
         */
        public boolean contains(Object o) {
            if ((from != null) && (from.equals(o)))
                return true;
            if ((to != null) && (to.equals(o)))
                return true;
            if (o != null)
                return false;
            return ((from == null) || (to == null));
        }

        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || !(getClass().isInstance(o))) {
                return false;
            }

            @SuppressWarnings("unchecked")
            ReconfigurationPair other = (ReconfigurationPair) o;

            return (from == null ? other.from == null : from.equals(other.from)) && (to == null ? other.to == null : to.equals(other.to));
        }
    }

    // Split the migration pairs into a series of splits, to be executed as sub-reconfigurations
    public static Map<Pair<Integer, Integer>, Integer> splitMigrationPairs(int numberOfSplits, 
		  Set<ReconfigurationPair> migrationPairs, AutoSplit autoSplit, int partitionsPerSite) {
        Map<Pair<Integer, Integer>, Integer> pairToSplitMapping = new HashMap<>();
        
        ArrayList<ReconfigurationPair> migrationPairsList = new ArrayList<>();
        migrationPairsList.addAll(migrationPairs);
        if (autoSplit != null) {
            int s = autoSplit.s;
            int parallel = Math.min(s, autoSplit.delta) * partitionsPerSite;
            int index = 0;
            int nMigrations = migrationPairsList.size() / parallel;
            if (s >= autoSplit.delta) { // Case 1
                for (ReconfigurationPair mPair : migrationPairsList) {
                    int min = Math.min(mPair.from, mPair.to) / partitionsPerSite;
                    int max = Math.max(mPair.from, mPair.to) / partitionsPerSite;
                    index = (min * (s-1) + max) % s;
                    pairToSplitMapping.put(new Pair<Integer, Integer>(mPair.from, mPair.to), index);
                }
            } else if (autoSplit.r == 0) { // Case 2                
                for (ReconfigurationPair mPair : migrationPairsList) {
                    int min = Math.min(mPair.from, mPair.to) / partitionsPerSite;
                    int max = Math.max(mPair.from, mPair.to) / partitionsPerSite;
                    index = (min * (s-1) + max) % s + s * ((max-s) / s);  
                    if (mPair.from > mPair.to) { // scale in
                        index = nMigrations - index - 1;
                    }
                    pairToSplitMapping.put(new Pair<Integer, Integer>(mPair.from, mPair.to), index);
                }
            } else { // Case 3      
                ArrayList<ReconfigurationPair> finalPhase = new ArrayList<>();
                for (ReconfigurationPair mPair : migrationPairsList) {
                    int min = Math.min(mPair.from, mPair.to) / partitionsPerSite;
                    int max = Math.max(mPair.from, mPair.to) / partitionsPerSite;
                    index = (min * (s-1) + max) % s + s * ((max-s) / s); 
                    if (index >= nMigrations - s) {
                        finalPhase.add(mPair);
                        continue;
                    }
                    if (mPair.from > mPair.to) { // scale in
                        index = nMigrations - index - 1;
                    }
                    pairToSplitMapping.put(new Pair<Integer, Integer>(mPair.from, mPair.to), index);
                }
                Collections.sort(finalPhase);
                int max = 0;
                for (ReconfigurationPair mPair : finalPhase) {
                    int min = Math.min(mPair.from, mPair.to) / partitionsPerSite;
                    index = (min * (s-1) + max) % s + nMigrations - s;
                    if (mPair.from > mPair.to) { // scale in
                        index = nMigrations - index - 1;
                    }
                    pairToSplitMapping.put(new Pair<Integer, Integer>(mPair.from, mPair.to), index);
                    max = (max + 1) % s;
                }
            }

        }
        else {
            // Sort the migration pairs to make sure we get the same set of splits
            // each time
            Collections.sort(migrationPairsList);

            // Split pairs into groups based on numberOfSplits param
            // desirable properties: 
            // - a partition should only be involved in one migration per split
            //    (e.g., if partition 1 sends to 2 and 3, <1,2> and <1,3> should be in different splits)           
            // - The migration pairs should be distributed evenly between the splits
            HashMap<Integer,Set<Integer>> splitPartitions = new HashMap<>();
            for (int i = 0; i < numberOfSplits; ++i) splitPartitions.put(i, new HashSet<Integer>());
            int[] pairsPerSplit = new int[numberOfSplits];

            int minPairsPerSplit = migrationPairsList.size() / numberOfSplits;
            for (ReconfigurationPair mPair : migrationPairsList) {
                int split = 0;
                while (splitPartitions.get(split).contains(mPair.from) || 
                        splitPartitions.get(split).contains(mPair.to) ||
                        pairsPerSplit[split] >= minPairsPerSplit + (split < migrationPairsList.size() % numberOfSplits ? 1 : 0)) {
                    split++;
                    if (split >= numberOfSplits) {
                        LOG.info("Not possible to split pairs optimally");
                        int min_pairs = migrationPairsList.size();
                        for (int i = 0; i < numberOfSplits; ++i) {
                            if (pairsPerSplit[i] < min_pairs) {
                                min_pairs = pairsPerSplit[i];
                                split = i;
                            }
                        }
                        break;
                    }
                }
                splitPartitions.get(split).add(mPair.from);
                splitPartitions.get(split).add(mPair.to);
                pairsPerSplit[split]++;
                pairToSplitMapping.put(new Pair<Integer, Integer>(mPair.from, mPair.to), split);
            }
        }

        return pairToSplitMapping;
    }
    
    public static class AutoSplit {
        int s = 0;
        int r = 0;
        int delta = 0;        
     
        // the number of splits needed to accommodate all migration pairs 
        // with maximum parallel migration
        int numberOfSplits = 0; 
        
        // extra splits for case 1 (delta < s)
        int extraSplits = 0;
        
        @Override
        public String toString() {
            return "Small cluster size (s): " + s + ", Remainder (r): " + r 
                    + ", Delta: " + delta + ", Number of splits: " + numberOfSplits
                    + ", Extra splits: " + extraSplits;
        }
    }
    
    public static AutoSplit getAutoSplit(ReconfigurationPlan plan, int partitionsPerSite, int numberOfSplits) {
        Set<Integer> incoming_partitions = new HashSet<>();
        for (Entry<Integer, List<ReconfigurationRange>> entry : plan.getIncoming_ranges().entrySet()) {
            if (!entry.getValue().isEmpty()) incoming_partitions.add(entry.getKey());
        }                   
        Set<Integer> outgoing_partitions = new HashSet<>();
        for (Entry<Integer, List<ReconfigurationRange>> entry : plan.getOutgoing_ranges().entrySet()) {
            if (!entry.getValue().isEmpty()) outgoing_partitions.add(entry.getKey());
        }
        
        Iterator<Integer> incoming_iter = incoming_partitions.iterator();
        Iterator<Integer> outgoing_iter = outgoing_partitions.iterator();
        AutoSplit as = new AutoSplit();
        
        if (!incoming_iter.hasNext() || !outgoing_iter.hasNext()) return as;
            
        if(incoming_iter.next() < outgoing_iter.next()) { // scale in
            as.s = incoming_partitions.size() / partitionsPerSite;
            as.delta = outgoing_partitions.size() / partitionsPerSite;
        }
        else { // scale out
            as.s = outgoing_partitions.size() / partitionsPerSite;
            as.delta = incoming_partitions.size() / partitionsPerSite;
        }
        
        as.r = as.delta % as.s;
        if (as.s > as.delta) { // case 1
            as.numberOfSplits = as.s;
        }
        else { // cases 2 and 3           
            as.numberOfSplits = as.s * (as.delta / as.s) + as.r;
        }

        // interleave many reconfigurations to achieve theoretical effective capacity
        as.extraSplits = as.numberOfSplits * numberOfSplits;
        
        LOG.info("AutoSplit: " + as.toString());
        return as;
    }
    
    public static List<ReconfigurationPlan> interleavePlans(List<ReconfigurationPlan> plans, int interleave) {
        List<ReconfigurationPlan> interleavedPlans = new ArrayList<>();
        int skip = plans.size() / interleave;
        for (int i = 0; i < skip; ++i) {
            for (int j = 0; j < interleave; ++j) {
                interleavedPlans.add(plans.get(i + j * skip));
            }
        }
        // add any remaining plans
        for (int i = interleavedPlans.size(); i < plans.size(); ++i) {
            interleavedPlans.add(plans.get(i));
        }
        
        debugSplits(interleavedPlans);
        return interleavedPlans;
    }
    
    // Print the migration pairs in each split (source and destination partitions)
    private static void debugSplits(List<ReconfigurationPlan> plans) {
        int j = 0;
        for (ReconfigurationPlan plan : plans) {

            Set<Pair<Integer, Integer>> debugSendingData = new HashSet<>();
            for (Entry<Integer, List<ReconfigurationRange>> entry : plan.getIncoming_ranges().entrySet()) {
                for (ReconfigurationRange range : entry.getValue()) {
                    debugSendingData.add(new Pair<Integer, Integer>(range.getOldPartition(), range.getNewPartition()));
                }
            }
            LOG.debug(String.format("PlanSplit:%s has the pairs(%s): %s", j, debugSendingData.size(), StringUtils.join(debugSendingData, ",")));
            ++j;
        }
    }
    
    
    public static List<ReconfigurationPlan> naiveSplitReconfigurationPlan(ReconfigurationPlan plan, int numberOfSplits, boolean autoSplit, int partitionsPerSite) {
        // The list of partition pairs that exchange data
        Set<ReconfigurationPair> migrationPairs = new HashSet<>();

        // Calculate pairs
        for (Entry<Integer, List<ReconfigurationRange>> entry : plan.getIncoming_ranges().entrySet()) {
            Set<Integer> partitionsSendingData = new HashSet<>();
            for (ReconfigurationRange range : entry.getValue()) {
                partitionsSendingData.add(range.getOldPartition());
                migrationPairs.add(new ReconfigurationPair(range.getOldPartition(), range.getNewPartition()));
            }
            LOG.info(String.format("Partition: %s is receiving data from :%s", entry.getKey(), StringUtils.join(partitionsSendingData, ",")));
        }
        LOG.info(String.format("Pairs(%s): %s ", migrationPairs.size(), StringUtils.join(migrationPairs, ",")));

        AutoSplit as = null;
        int extraSplits = 0;
        if (autoSplit) {
            as = getAutoSplit(plan, partitionsPerSite, numberOfSplits);
            numberOfSplits = as.numberOfSplits;
            extraSplits = as.extraSplits;
        } else {        
            // Limit the number of splits to number of pairs
            if (numberOfSplits > migrationPairs.size()) {
                extraSplits = numberOfSplits - migrationPairs.size();
                numberOfSplits = migrationPairs.size();
                LOG.info("Limiting number of pair splits to " + numberOfSplits);
            }
        }

        // Now split up the pairs
        Map<Pair<Integer, Integer>, Integer> pairToSplitMapping = splitMigrationPairs(numberOfSplits, migrationPairs, as, partitionsPerSite);
        
        List<ReconfigurationPlan> splitPlans = new ArrayList<>();
        for (int i = 0; i < numberOfSplits; i++) {
            splitPlans.add(new ReconfigurationPlan(plan.getCatalogContext(), plan.getPartitionedTablesByFK()));
        }

        // put ranges into split rangePLans
        for (Entry<Integer, List<ReconfigurationRange>> entry : plan.getIncoming_ranges().entrySet()) {
            for (ReconfigurationRange range : entry.getValue()) {
                // find which split this range is going into
                Integer splitIndex = pairToSplitMapping.get(new Pair<Integer, Integer>(range.getOldPartition(), range.getNewPartition()));
                // add it
                splitPlans.get(splitIndex).addRange(range);
            }
        }

        debugSplits(splitPlans);

        if (splitPlans.size() > 0 && extraSplits > 0) {
            int extraSplitsPerPlan = extraSplits / splitPlans.size();
            int extraSplitsRemainder = extraSplits % splitPlans.size();
            List<ReconfigurationPlan> splitPlansAgain = new ArrayList<>();
            for (ReconfigurationPlan splitPlan : splitPlans) {
                int extra = (extraSplitsRemainder > 0 ? 2 : 1);
                splitPlansAgain.addAll(fineGrainedSplitReconfigurationPlan(splitPlan, extraSplitsPerPlan + extra));
                extraSplitsRemainder--;
            }
            // For case 1, interleave plans
            if (autoSplit && as.s > as.delta) splitPlansAgain = interleavePlans(splitPlansAgain, numberOfSplits);
            return splitPlansAgain;
        }

        return splitPlans;
    }

    private static List<ReconfigurationRange> splitReconfigurationRangeOnPartitionKeys(ReconfigurationRange range, String table_name, Pair<Object[], Object[]> subKeyMinMax, long maxSplits) {
        if (maxSplits <= 1 || subKeyMinMax == null) {
            return Arrays.asList(range);
        }

        // HACK for TPCC
        if (table_name.equals("orders")) {
            maxSplits *= 3;
        }

        List<ReconfigurationRange> res = new ArrayList<>();
        LOG.info("Old range: " + range.toString());
        VoltTable temp = range.getKeySchema().clone(0);
        long min_long = ((Number) range.getMinIncl().get(0)[0]).longValue();
        long max_long = ((Number) range.getMaxExcl().get(0)[0]).longValue();
        Object[] min = range.getMinIncl().get(0).clone();
        Object[] max = null;
        List<Object[]> keySplits = getKeySplits(min_long, max_long, subKeyMinMax, maxSplits);

        // remove first element
        keySplits.remove(0);

        for (Object[] keySplit : keySplits) {
            max = new Object[temp.getColumnCount()];
            for (int i = 0; i < max.length && i < keySplit.length; i++) {
                max[i] = keySplit[i];
            }
            for (int i = keySplit.length; i < max.length; i++) {
                VoltType vt = temp.getColumnType(i);
                max[i] = vt.getNullValue();
            }
            temp.addRow(max);
            temp.advanceToRow(0);
            max = temp.getRowArray();
            temp.clearRowData();
            res.add(new ReconfigurationRange(table_name, range.getKeySchema(), min, max, range.getOldPartition(), range.getNewPartition()));
            LOG.info("New range: " + res.get(res.size() - 1).toString());
            min = max;
        }

        max = range.getMaxExcl().get(0).clone();
        res.add(new ReconfigurationRange(table_name, range.getKeySchema(), min, max, range.getOldPartition(), range.getNewPartition()));
        LOG.info("New range: " + res.get(res.size() - 1).toString());

        return res;
    }

    private static List<Object[]> getKeySplits(long keyMin, long keyMax, Pair<Object[], Object[]> subKeyMinMax, long maxSplits) {
        List<Object[]> keySplits = new ArrayList<>();
        Object[] min = subKeyMinMax.getFirst();
        Object[] max = subKeyMinMax.getSecond();
        assert (min.length == max.length) : "Min and max sub keys must be the same length.  Min length: " + min.length + " Max length: " + max.length;

        // calculate the values of min key, number of splits, and range size for
        // each level
        long minArray[] = new long[min.length + 1];
        long rangeSizeArray[] = new long[min.length + 1];
        long numSplitsArray[] = new long[min.length + 1];
        int splitLength = 1;

        minArray[0] = keyMin;
        long numKeys = keyMax - keyMin;
        if (numKeys == 0L)
            numKeys = 1L;

        rangeSizeArray[0] = (numKeys >= maxSplits ? numKeys / maxSplits : 1L);
        numSplitsArray[0] = numKeys / rangeSizeArray[0];
        long totalSplits = numSplitsArray[0];

        for (int i = 0; i < min.length && totalSplits < maxSplits; ++i) {

            minArray[i + 1] = ((Number) min[i]).longValue();
            numKeys = ((Number) max[i]).longValue() - minArray[i + 1];
            if (numKeys == 0L)
                numKeys = 1L;

            rangeSizeArray[i + 1] = (totalSplits * numKeys >= maxSplits ? totalSplits * numKeys / maxSplits : 1L);
            numSplitsArray[i + 1] = numKeys / rangeSizeArray[i + 1];
            totalSplits *= numSplitsArray[i + 1];

            splitLength++;
        }

        // keep track of the key index of each level which will be used
        // to make the next split
        int counterArray[] = new int[min.length + 1];

        for (int i = 0; i < Math.min(totalSplits, maxSplits); ++i) {
            // Run through the inner lists, grabbing the member from the list
            // specified by the counterArray for each inner list, and build a
            // combination list.
            Object[] split = new Object[splitLength];
            for (int j = 0; j < splitLength; ++j) {
                split[j] = minArray[j] + counterArray[j] * rangeSizeArray[j];
            }
            keySplits.add(split); // add new split to list

            // Now we need to increment the counterArray so that the next
            // combination is taken on the next iteration of this loop.
            for (int incIndex = splitLength - 1; incIndex >= 0; --incIndex) {
                if (counterArray[incIndex] + 1 < numSplitsArray[incIndex]) {
                    ++counterArray[incIndex];
                    // None of the indices of higher significance need to be
                    // incremented, so jump out of this for loop at this point.
                    break;
                }
                // The index at this position is at its max value, so zero it
                // and continue this loop to increment the index which is more
                // significant than this one.
                counterArray[incIndex] = 0;
            }
        }

        return keySplits;
    }

    public static Number getFirstNumber(VoltTable vt) {
        try {
            vt.resetRowPosition();
            vt.advanceRow();
            return (Number) vt.get(0);
        } catch (Exception e) {
            LOG.error(e);
        }
        return new Long(-1);
    }

    public static Pair<Number, Number> getFirst(ReconfigurationRange range) {
        // TODO Auto-generated method stub
        try {
            Number min = (Number) range.getMinIncl().get(0)[0];
            Number max = (Number) range.getMaxExcl().get(0)[0];
            return new Pair<Number, Number>(min, max);
        } catch (Exception e) {
            LOG.error(e);
        }
        return new Pair<Number, Number>(new Long(-1), new Long(-1));
    }

    public static List<ReconfigurationPlan> fineGrainedSplitReconfigurationPlan(ReconfigurationPlan plan, int numberOfSplits) {

        LOG.debug("splitting into " + numberOfSplits + " plans starting with plan: \n Out: " + 
                plan.getOutgoing_ranges().toString() + " \n In: " + plan.getIncoming_ranges().toString());
        
        if (numberOfSplits <= 1) {
            return Arrays.asList(plan);
        }

        // Find the explicitly partitioned tables and their ranges
        HashMap<String, List<String>> explicitPartitionedTables = new HashMap<>();
        List<ReconfigurationRange> explicitPartitionedTablesRanges = new ArrayList<>();
        for (List<ReconfigurationRange> ranges : plan.getIncoming_ranges().values()) {
            for (ReconfigurationRange range : ranges) {
                String table = range.getTableName();
                if (!plan.getPartitionedTablesByFK().containsKey(table)) {
                    explicitPartitionedTablesRanges.add(range);
                    if (!explicitPartitionedTables.containsKey(table)) {
                        explicitPartitionedTables.put(table, new ArrayList<String>());
                    }
                }
            }
        }
        
        // find reverse map of fk partitioning
        for (Entry<String, String> entry : plan.getPartitionedTablesByFK().entrySet()) {
            List<String> table = explicitPartitionedTables.get(entry.getValue());
            if (table != null) {
                table.add(entry.getKey());
            }
        }
        
        int numRanges = explicitPartitionedTablesRanges.size();        
        List<ReconfigurationPlan> splitPlans = new ArrayList<>();
        if (numRanges == 0) return splitPlans;
        
        // If more ranges than splits, no need to split up the ranges
        if (numRanges > numberOfSplits) {
            for (int i = 0; i < numberOfSplits; i++) {
                splitPlans.add(new ReconfigurationPlan(plan.getCatalogContext(), plan.getPartitionedTablesByFK()));
            }

            // sort the ranges
            Collections.sort(explicitPartitionedTablesRanges, numericReconfigurationRangeComparator);
            
            // combine ranges from related tables
            int rangeIndex = 0;
            for (ReconfigurationRange range : explicitPartitionedTablesRanges) {
                
                int splitIndex = rangeIndex % numberOfSplits;
                ReconfigurationPlan newPlan = splitPlans.get(splitIndex);

                newPlan.addRange(range);
                for (String table_name : explicitPartitionedTables.get(range.getTableName())) {
                    newPlan.addRange(range.clone(plan.getCatalogContext().getTableByName(table_name)));
                }
                
                rangeIndex++;
            }
            
        }
        else {
            // find the splits for explicitly partitioned tables
            List<ReconfigurationRange> explicitPartitionedTablesSplitRanges = new ArrayList<>();
            long splitsPerRange = numberOfSplits / numRanges;
            long splitsRemainder = numberOfSplits % numRanges;
            for (ReconfigurationRange range : explicitPartitionedTablesRanges) {
                int extra = (splitsRemainder > 0 ? 2 : 1);
                List<ReconfigurationRange> splitRanges = splitReconfigurationRangeOnPartitionKeys(range, range.getTableName(), getSubKeyMinMax(range.getTableName(), plan.getPartitionedTablesByFK()),
                        splitsPerRange + extra);
                splitsRemainder--;

                explicitPartitionedTablesSplitRanges.addAll(splitRanges);
            }

            // sort the ranges
            Collections.sort(explicitPartitionedTablesSplitRanges, numericReconfigurationRangeComparator);

            // combine ranges from related tables
            for (ReconfigurationRange range : explicitPartitionedTablesSplitRanges) {
                ReconfigurationPlan newPlan = new ReconfigurationPlan(plan.getCatalogContext(), plan.getPartitionedTablesByFK());

                newPlan.addRange(range);
                for (String table_name : explicitPartitionedTables.get(range.getTableName())) {
                    newPlan.addRange(range.clone(plan.getCatalogContext().getTableByName(table_name)));
                }
                
                splitPlans.add(newPlan);
            }
        }
        
        if (debug.val) {
            for (ReconfigurationPlan newPlan : splitPlans) {
                LOG.debug("New plan: \n Out: " + newPlan.getOutgoing_ranges().toString() + " \n In: " + newPlan.getIncoming_ranges().toString());
            }
        }

        return splitPlans;
    }

    public static Pair<Object[], Object[]> getSubKeyMinMax(String table_name, Map<String, String> partitionedTablesByFK) {
        LOG.debug("getSubKeyMinMax");
        // HACK - this is currently hard coded for TPCC
        String partitionedTable = partitionedTablesByFK.get(table_name);
        if (partitionedTable == null) {
            partitionedTable = table_name;
        }
        Pair<Object[], Object[]> res = null;
        // <min incl, max excl>
        if (partitionedTable.equals("district")) {
            res = new Pair<Object[], Object[]>(new Object[] { 1 }, new Object[] { 11 });
        } else if (partitionedTable.equals("customer")) {
            res = new Pair<Object[], Object[]>(new Object[] { 1, 0 }, new Object[] { 11, 60000 });
        } else if (partitionedTable.equals("orders")) {
            res = new Pair<Object[], Object[]>(new Object[] { 1, 0 }, new Object[] { 11, 6000 });
        } else if (partitionedTable.equals("stock")) {
            res = new Pair<Object[], Object[]>(new Object[] { 0 }, new Object[] { 100000 });
        }
        return res;
    }

    private static Comparator<Object[]> numericVoltTableComparator = new Comparator<Object[]>() {

        @Override
        public int compare(Object[] o1, Object[] o2) {
            assert (o1 != null);
            assert (o2 != null);
            int cmp = 0;
            for (int i = 0; i < o1.length && i < o2.length; i++) {

                cmp = (int) (new Long(((Number) o1[i]).longValue())).compareTo(new Long(((Number) o2[i]).longValue()));

                if (cmp != 0)
                    break;
            } // FOR

            if (cmp == 0) {
                if (o1.length < o2.length) {
                    return -1;
                } else if (o1.length > o2.length) {
                    return 1;
                }
            }

            // TODO: Handle duplicates!
            return (cmp);
        }
    };

    public static Comparator<ReconfigurationRange> numericReconfigurationRangeComparator = new Comparator<ReconfigurationRange>() {
        public int compare(ReconfigurationRange r1, ReconfigurationRange r2) {
            if (numericVoltTableComparator.compare(r1.getMinIncl().get(0), r2.getMinIncl().get(0)) < 0) {
                return -1;
            } else if (numericVoltTableComparator.compare(r1.getMinIncl().get(0), r2.getMinIncl().get(0)) == 0) {
                return numericVoltTableComparator.compare(r1.getMaxExcl().get(0), r2.getMaxExcl().get(0));
            } else {
                return 1;
            }
        }
    };

    public static VoltTable getExtractVoltTable(List<ReconfigurationRange> ranges) {
        ReconfigurationRange sample = ranges.get(0);
        ReconfigurationRange mergedRanges = new ReconfigurationRange(sample.getTableName(), sample.getKeySchema(), new ArrayList<Object[]>(), new ArrayList<Object[]>(), sample.getOldPartition(),
                sample.getNewPartition());

        for (ReconfigurationRange range : ranges) {
            List<Object[]> max = range.getMaxExcl();
            List<Object[]> min = range.getMinIncl();
            mergedRanges.getMaxExcl().addAll(max);
            mergedRanges.getMinIncl().addAll(min);
            mergedRanges.truncateNullCols();
            mergedRanges.updateMinMax();
        }

        return getExtractVoltTable(mergedRanges);
    }

    public static VoltTable getExtractVoltTable(ReconfigurationRange range) {
        return getExtractVoltTable(range.getKeySchema(), range.getMinIncl(), range.getMaxExcl());
    }

    public static VoltTable getExtractVoltTable(VoltTable minIncl, VoltTable maxExcl) {
        List<Object[]> minInclList = new ArrayList<Object[]>();
        List<Object[]> maxExclList = new ArrayList<Object[]>();

        minIncl.resetRowPosition();
        maxExcl.resetRowPosition();
        while (minIncl.advanceRow() && maxExcl.advanceRow()) {
            minInclList.add(minIncl.getRowArray());
            maxExclList.add(maxExcl.getRowArray());
        }
        return getExtractVoltTable(minIncl, minInclList, maxExclList);
    }

    public static VoltTable getExtractVoltTable(VoltTable keySchema, List<Object[]> minIncl, List<Object[]> maxExcl) {
        int nCols = keySchema.getColumnCount();
        int nSchemaCols = nCols * 3 + 1;
        ColumnInfo[] extractTableColumns = new ColumnInfo[nSchemaCols];

        extractTableColumns[0] = new ColumnInfo("TABLE_NAME", VoltType.INTEGER);
        for (int i = 0; i < nCols; i++) {
            VoltType type = keySchema.getColumnType(i);
            extractTableColumns[i * 3 + 1] = new ColumnInfo("KEY_TYPE", VoltType.INTEGER);
            extractTableColumns[i * 3 + 2] = new ColumnInfo("MIN_INCLUSIVE", type); // range.getVt());
            extractTableColumns[i * 3 + 3] = new ColumnInfo("MAX_EXCLUSIVE", type); // range.getVt());
        }

        VoltTable vt = new VoltTable(extractTableColumns);
        // vt.addRow(range.table_name,range.getVt().toString(),range.getMin_inclusive(),range.getMax_exclusive());

        for (int i = 0; i < minIncl.size() && i < maxExcl.size(); i++) {
            Object[] min_incl_i = minIncl.get(i);
            Object[] max_excl_i = maxExcl.get(i);
            Object[] row = new Object[nSchemaCols];
            row[0] = 1;
            for (int j = 0; j < nCols; j++) {
                VoltType type = keySchema.getColumnType(j);
                Object null_val = type.getNullValue();
                row[j * 3 + 1] = 1;
                row[j * 3 + 2] = (j < min_incl_i.length ? min_incl_i[j] : null_val);
                row[j * 3 + 3] = (j < max_excl_i.length ? max_excl_i[j] : null_val);
            }
            vt.addRow(row);
        }

        return vt;
    }

    public static ReconfigurationRange getReconfigurationRange(Table table, Long[] mins, Long[] maxs, int old_partition, int new_partition) {
        return getReconfigurationRange(table, new Long[][] { mins }, new Long[][] { maxs }, old_partition, new_partition);
    }

    public static ReconfigurationRange getReconfigurationRange(Table table, Long[][] mins, Long[][] maxs, int old_partition, int new_partition) {
        VoltTable clone = getPartitionKeysVoltTable(table);

        ArrayList<Object[]> min_rows = new ArrayList<Object[]>();
        ArrayList<Object[]> max_rows = new ArrayList<Object[]>();
        for (int i = 0; i < mins.length && i < maxs.length; i++) {
            Long[] minsSubKeys = mins[i];
            Long[] maxsSubKeys = maxs[i];
            Object[] min_row = new Object[clone.getColumnCount()];
            Object[] max_row = new Object[clone.getColumnCount()];
            int col = 0;
            for (; col < minsSubKeys.length && col < maxsSubKeys.length && col < clone.getColumnCount(); col++) {
                min_row[col] = minsSubKeys[col];
                max_row[col] = maxsSubKeys[col];
            }
            for (; col < clone.getColumnCount(); col++) {
                VoltType vt = clone.getColumnType(col);
                Object obj = vt.getNullValue();
                min_row[col] = obj;
                max_row[col] = obj;
            }
            min_rows.add(min_row);
            max_rows.add(max_row);
        }

        VoltTable min_incl = clone.clone(0);
        VoltTable max_excl = clone.clone(0);
        for (Object[] row : min_rows) {
            min_incl.addRow(row);
        }
        for (Object[] row : max_rows) {
            max_excl.addRow(row);
        }

        return new ReconfigurationRange(table.getName(), clone, min_incl, max_excl, old_partition, new_partition);
    }

    public static VoltTable getVoltTable(Table table, List<Long> list) {
        VoltTable voltTable = getPartitionKeysVoltTable(table);

        for (int i = 0; i < list.size(); i++) {
            Object[] row = new Object[voltTable.getColumnCount()];

            row[0] = list.get(i);
            for (int col = 1; col < voltTable.getColumnCount(); col++) {
                VoltType vt = voltTable.getColumnType(col);
                Object obj = vt.getNullValue();
                row[col] = obj;
            }
            voltTable.addRow(row);
        }

        return voltTable;
    }

    public static VoltTable getPartitionKeysVoltTable(Table table) {

        Column[] cols;
        if (table.getPartitioncolumns().size() > 0) {
            cols = new Column[table.getPartitioncolumns().size()];
            for (ColumnRef colRef : table.getPartitioncolumns()) {
                cols[colRef.getIndex()] = colRef.getColumn();
            }
        } else if (table.getPartitioncolumn() != null) {
            cols = new Column[] { table.getPartitioncolumn() };
        } else {
            throw new RuntimeException("No partition columns provided for table " + table.getName());
        }
        return CatalogUtil.getVoltTable(Arrays.asList(cols));
    }

}
