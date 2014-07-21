package edu.brown.hstore.reconfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
import edu.brown.utils.CollectionUtil;

public class ReconfigurationUtil {
    private static final Logger LOG = Logger.getLogger(ReconfigurationUtil.class);
    
    public static class ReconfigurationPair implements Comparable<ReconfigurationPair> {
    	public Integer from;
    	public Integer to;
    	
    	public ReconfigurationPair(Integer from, Integer to) {
    		this.from = from;
    		this.to = to;
        }
    	
    	// order the pairs so that the lowest partition sends to the highest partition first
    	@Override
    	public int compareTo(ReconfigurationPair other) {
            if(this.from < other.from) {
            	return -1;
            }
            else if(this.from > other.from) {
            	return 1;
            }
            else if(this.to < other.to) {
            	return 1;
            }
            else if(this.to > other.to) {
            	return -1;
            }
            return 0;
        }
    	
    	public String toString() {
            return String.format("(%s->%s)", from, to);
        }
    	
	@Override
	public final int hashCode() {
	    return (from == null ? 0 : from.hashCode() * 31) +
		(to == null ? 0 : to.hashCode());
	}

        /**
        * @param o Object to compare to.
        * @return Is the object equal to a value in the pair.
        */
        public boolean contains(Object o) {
            if ((from != null) && (from.equals(o))) return true;
            if ((to != null) && (to.equals(o))) return true;
            if (o != null) return false;
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

            return (from == null ? other.from == null : from.equals(other.from))
                    && (to == null ? other.to == null : to.equals(other.to));
        }
    }

    public static List<ReconfigurationPlan> naiveSplitReconfigurationPlan(ReconfigurationPlan plan, int numberOfSplits){
        //The list of partition pairs that exchange data
        Set<ReconfigurationPair> migrationPairs = new HashSet<>();
        
        //How we are going to split up the pairs
        Map<Pair<Integer,Integer>, Integer> pairToSplitMapping = new HashMap<>();
        
        //Calculate pairs
        for(Entry<Integer, List<ReconfigurationRange>> entry : plan.getIncoming_ranges().entrySet()){
            Set<Integer> partitionsSendingData = new HashSet<>();
            for(ReconfigurationRange range : entry.getValue()){
                partitionsSendingData.add(range.getOldPartition());
                migrationPairs.add(new ReconfigurationPair(range.getOldPartition(), range.getNewPartition()));
            }
            LOG.info(String.format("Partition: %s is receiving data from :%s",entry.getKey(),StringUtils.join(partitionsSendingData, ",")));
        }
        LOG.info(String.format("Pairs(%s): %s ",migrationPairs.size(), StringUtils.join(migrationPairs, ",")));

        // Sort the migration pairs to make sure we get the same set of splits each time
        ArrayList<ReconfigurationPair> migrationPairsList = new ArrayList<>();
        migrationPairsList.addAll(migrationPairs);
        Collections.sort(migrationPairsList);
        
        //Limit the number of splits to number of pairs
        int extraSplits = 0;
        if (numberOfSplits > migrationPairsList.size()){
        	extraSplits = numberOfSplits - migrationPairsList.size();
            numberOfSplits = migrationPairsList.size();
            LOG.info("Limiting number of pair splits to " + numberOfSplits);
        }
        
        //Split pairs into groups based on numSplits paramt
        int pairCounter = 0;
        for(ReconfigurationPair mPair : migrationPairsList){
            pairToSplitMapping.put(new Pair<Integer, Integer>(mPair.from, mPair.to), pairCounter % numberOfSplits);
            pairCounter++;
        }

        List<ReconfigurationPlan> splitPlans = new ArrayList<>();
        for(int i = 0; i < numberOfSplits; i++){
            splitPlans.add(new ReconfigurationPlan(plan.getCatalogContext(), plan.getPartitionedTablesByFK()));
        }

        
        //put ranges into split rangePLans
        for(Entry<Integer, List<ReconfigurationRange>> entry : plan.getIncoming_ranges().entrySet()){
            for(ReconfigurationRange range : entry.getValue()){
                //find which split this range is going into
                Integer splitIndex = pairToSplitMapping.get(new Pair<Integer, Integer>(range.getOldPartition(), range.getNewPartition()));
                //add it
                splitPlans.get(splitIndex).addRange(range);
            }
        }

        //DEBUG
        for (int j = 0; j < numberOfSplits; j++){
            
            Set<Pair<Integer,Integer>> debugSendingData = new HashSet<>();
            for(Entry<Integer, List<ReconfigurationRange>> entry : splitPlans.get(j).getIncoming_ranges().entrySet()){
                for(ReconfigurationRange range : entry.getValue()){
                    debugSendingData.add(new Pair<Integer, Integer>(range.getOldPartition(), range.getNewPartition()));
                }
            }
            LOG.info(String.format("PlanSplit:%s has the pairs(%s): %s", j, debugSendingData.size(), StringUtils.join(debugSendingData,",")));
        }
        
        if(splitPlans.size() > 0 && extraSplits > 0) {
        	int extraSplitsPerPlan = extraSplits / splitPlans.size();
        	int extraSplitsRemainder = extraSplits % splitPlans.size();
        	List<ReconfigurationPlan> splitPlansAgain = new ArrayList<>();
        	for(ReconfigurationPlan splitPlan : splitPlans) {
        		int extra = (extraSplitsRemainder > 0 ? 2 : 1);
        		splitPlansAgain.addAll(fineGrainedSplitReconfigurationPlan(splitPlan, extraSplitsPerPlan + extra));
        		extraSplitsRemainder--;
        	}
        	return splitPlansAgain;

        }
        
        return splitPlans;
    }
    
    private static List<ReconfigurationRange> splitReconfigurationRangeOnPartitionKeys(ReconfigurationRange range, String table_name, Pair<Object[], Object[]> subKeyMinMax, long maxSplits) {
    	if(maxSplits <= 1 || subKeyMinMax == null) {
    		return Arrays.asList(range);
    	}
    	
    	// HACK for TPCC
//    	if(table_name.equals("orders")) {
//    		maxSplits *= 10;
//    	}
    	
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
    
    	for(Object[] keySplit : keySplits) {
    		max = new Object[min.length];
    		for(int i = 0; i < max.length && i < keySplit.length; i++) {
    			max[i] = keySplit[i];
    		}
    		for(int i = keySplit.length; i < max.length; i++) {
    			VoltType vt = temp.getColumnType(i);
    			max[i] = vt.getNullValue();
    		}
    		temp.addRow(max);
    		temp.advanceToRow(0);
    		max = temp.getRowArray();
    		temp.clearRowData();
    		res.add(new ReconfigurationRange(table_name, range.getKeySchema(), min, max, range.getOldPartition(), range.getNewPartition()));
    		LOG.info("New range: " + res.get(res.size()-1).toString());
    		min = max;
    	}

    	max = range.getMaxExcl().get(0).clone();
    	res.add(new ReconfigurationRange(table_name, range.getKeySchema(), min, max, range.getOldPartition(), range.getNewPartition()));
    	LOG.info("New range: " + res.get(res.size()-1).toString());
		
        return res;
    }
    
    private static List<Object[]> getKeySplits(long keyMin, long keyMax, Pair<Object[], Object[]> subKeyMinMax, long maxSplits) {
    	List<Object[]> keySplits = new ArrayList<>();
    	Object[] min = subKeyMinMax.getFirst();
    	Object[] max = subKeyMinMax.getSecond();
    	assert(min.length == max.length) : "Min and max sub keys must be the same length.  Min length: " + min.length + " Max length: " + max.length;
    	
    	// calculate the values of min key, number of splits, and range size for each level
    	long minArray[] = new long[min.length + 1];
    	long rangeSizeArray[] = new long[min.length + 1];
    	long numSplitsArray[] = new long[min.length + 1];
    	int splitLength = 1;

    	minArray[0] = keyMin;
    	long numKeys = keyMax - keyMin;
    	if (numKeys == 0L) numKeys = 1L;
    	
    	rangeSizeArray[0] = (numKeys >= maxSplits ? numKeys / maxSplits : 1L); 
    	numSplitsArray[0] = numKeys / rangeSizeArray[0];
    	long totalSplits = numSplitsArray[0];

    	for(int i = 0; i < min.length && totalSplits < maxSplits; ++i) {

	    minArray[i + 1] = ((Number) min[i]).longValue();
	    numKeys = ((Number) max[i]).longValue() - minArray[i + 1];
    		if (numKeys == 0L) numKeys = 1L;

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
    		for(int j = 0; j < splitLength; ++j) {
    			split[j] = minArray[j] + counterArray[j] * rangeSizeArray[j];
    		}
    		keySplits.add(split);  // add new split to list

    		// Now we need to increment the counterArray so that the next
    		// combination is taken on the next iteration of this loop.
    		for(int incIndex = splitLength - 1; incIndex >= 0; --incIndex) {
    			if(counterArray[incIndex] + 1 < numSplitsArray[incIndex]) {
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
    
    public static Number getFirstNumber(VoltTable vt){
        try{
            vt.resetRowPosition();
            vt.advanceRow();       
            return (Number)vt.get(0);
        } catch (Exception e){
            LOG.error(e);
        }
        return new Long(-1);
    }
    
    public static Pair<Number, Number> getFirst(ReconfigurationRange range) {
        // TODO Auto-generated method stub
        try{
            Number min= (Number)range.getMinIncl().get(0)[0];
            Number max = (Number)range.getMaxExcl().get(0)[0];
            return new Pair<Number,Number>(min,max);
        } catch (Exception e) {
            LOG.error(e);
        }
        return new Pair<Number,Number>(new Long(-1), new Long(-1));
    }
    
    public static List<ReconfigurationPlan> fineGrainedSplitReconfigurationPlan(ReconfigurationPlan plan, int numberOfSplits){
    	
    	if(numberOfSplits <= 1) {
    		return Arrays.asList(plan);
    	}
    	
    	List<ReconfigurationRange> allRanges = new ArrayList<>();
    	for(List<ReconfigurationRange> ranges : plan.getIncoming_ranges().values()) {
    		allRanges.addAll(ranges);
    	}
    	
    	List<ReconfigurationPlan> splitPlans = new ArrayList<>();
    	
    	int numRanges = 0;
    	for(ReconfigurationRange range : allRanges) {
    		if(plan.getPartitionedTablesByFK().get(range.getTableName()) == null) {
    			numRanges++;
    		}
    	}
    	
    	// find the splits for explicitly partitioned tables
    	HashMap<String, List<String>> explicitPartitionedTables = new HashMap<>();
    	List<ReconfigurationRange> explicitPartitionedTablesSplitRanges = new ArrayList<>();
    	long splitsPerRange = numberOfSplits / numRanges;
    	long splitsRemainder = numberOfSplits % numRanges;
    	for(ReconfigurationRange range : allRanges) {
    		if(plan.getPartitionedTablesByFK().get(range.getTableName()) == null) {
    			int extra = (splitsRemainder > 0 ? 2 : 1);
    			List<ReconfigurationRange> splitRanges = splitReconfigurationRangeOnPartitionKeys(range, range.getTableName(), 
    					getSubKeyMinMax(range.getTableName(), plan.getPartitionedTablesByFK()), splitsPerRange + extra); 
    			splitsRemainder--;
    			
    			if(explicitPartitionedTables.get(range.getTableName()) == null) {
    				explicitPartitionedTables.put(range.getTableName(), new ArrayList<String>());
        		}
    			explicitPartitionedTablesSplitRanges.addAll(splitRanges);
    		}
    	}
    	
    	// find reverse map of fk partitioning
    	for(Entry<String, String> entry : plan.getPartitionedTablesByFK().entrySet()) {
    		explicitPartitionedTables.get(entry.getValue()).add(entry.getKey());
    	}
    	
    	// sort the ranges
    	Collections.sort(explicitPartitionedTablesSplitRanges, numericReconfigurationRangeComparator);

    	// combine ranges from related tables
    	for(ReconfigurationRange range : explicitPartitionedTablesSplitRanges) {
    		ReconfigurationPlan newPlan = new ReconfigurationPlan(plan.getCatalogContext(), plan.getPartitionedTablesByFK());
    		
			newPlan.addRange(range);
			for(String table_name : explicitPartitionedTables.get(range.getTableName())) {
				newPlan.addRange(range.clone(plan.getCatalogContext().getTableByName(table_name)));
			}
			
			splitPlans.add(newPlan);
		}
        
        return splitPlans;
    }
    
    public static Pair<Object[], Object[]> getSubKeyMinMax(String table_name, Map<String, String> partitionedTablesByFK) {
        LOG.info("getSubKeyMinMax");
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

            if(cmp == 0) {
            	if(o1.length < o2.length) {
            		return -1;
            	} else if (o1.length > o2.length) {
            		return 1;
            	}
            }
            
            // TODO: Handle duplicates!
            return (cmp);
        }
    };
    
    private static Comparator<ReconfigurationRange> numericReconfigurationRangeComparator = new Comparator<ReconfigurationRange>() {
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
    	ReconfigurationRange mergedRanges = new ReconfigurationRange(sample.getTableName(), sample.getKeySchema(), new ArrayList<Object[]>(), new ArrayList<Object[]>(), sample.getOldPartition(), sample.getNewPartition());
		
		for(ReconfigurationRange range : ranges) {
			List<Object[]> max = range.getMaxExcl();
			List<Object[]> min = range.getMinIncl();
			mergedRanges.getMaxExcl().addAll(max);
			mergedRanges.getMinIncl().addAll(min);
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
    	while(minIncl.advanceRow() && maxExcl.advanceRow()) {
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
        for(int i = 0; i < nCols; i++) {
        	VoltType type = keySchema.getColumnType(i);
        	extractTableColumns[i*3+1] = new ColumnInfo("KEY_TYPE", VoltType.INTEGER);
            extractTableColumns[i*3+2] = new ColumnInfo("MIN_INCLUSIVE", type); // range.getVt());
            extractTableColumns[i*3+3] = new ColumnInfo("MAX_EXCLUSIVE", type); // range.getVt());
        }  
        
        VoltTable vt = new VoltTable(extractTableColumns);
        // vt.addRow(range.table_name,range.getVt().toString(),range.getMin_inclusive(),range.getMax_exclusive());
        
        for(int i = 0; i < minIncl.size() && i < maxExcl.size(); i++) {
        	Object[] min_incl_i = minIncl.get(i);
        	Object[] max_excl_i = maxExcl.get(i);
			Object[] row = new Object[nSchemaCols];
    		row[0] = 1;
    		for(int j = 0; j < nCols; j++) {	
        		row[j*3+1] = 1;
        		row[j*3+2] = min_incl_i[j];
        		row[j*3+3] = max_excl_i[j];
        	}
    		vt.addRow(row);
        }  
        
		return vt;
    }
    
    public static ReconfigurationRange getReconfigurationRange(Table table, Long[] mins, 
    		Long[] maxs, int old_partition, int new_partition) {
    	return getReconfigurationRange(table, new Long[][]{mins}, new Long[][]{maxs}, old_partition, new_partition);
    }
    
    public static ReconfigurationRange getReconfigurationRange(Table table, Long[][] mins, 
    		Long[][] maxs, int old_partition, int new_partition) {
    	VoltTable clone = getPartitionKeysVoltTable(table);

        ArrayList<Object[]> min_rows = new ArrayList<Object[]>();
        ArrayList<Object[]> max_rows = new ArrayList<Object[]>();
        for(int i = 0; i < mins.length && i < maxs.length; i++) {
        	Long[] minsSubKeys = mins[i];
        	Long[] maxsSubKeys = maxs[i];
        	Object[] min_row = new Object[clone.getColumnCount()];
    		Object[] max_row = new Object[clone.getColumnCount()];
    		int col = 0;
    		for( ; col < minsSubKeys.length && col < maxsSubKeys.length && col < clone.getColumnCount(); col++) {
        		min_row[col] = minsSubKeys[col];
        		max_row[col] = maxsSubKeys[col];
        	}
    		for ( ; col < clone.getColumnCount(); col++) {
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
        for(Object[] row : min_rows) {
        	min_incl.addRow(row);
        }
        for(Object[] row : max_rows) {
        	max_excl.addRow(row);
        }

        return new ReconfigurationRange(table.getName(), clone, min_incl, max_excl, old_partition, new_partition);
    }
    
    public static VoltTable getVoltTable(Table table, List<Long> list) {
    	VoltTable voltTable = getPartitionKeysVoltTable(table);
        
        for(int i = 0; i < list.size(); i++) {
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
    	if(table.getPartitioncolumns().size() > 0) {
    		cols = new Column[table.getPartitioncolumns().size()];
    		for(ColumnRef colRef : table.getPartitioncolumns()) {
    			cols[colRef.getIndex()] = colRef.getColumn();
    		}
    	} else if (table.getPartitioncolumn() != null) {
    		cols = new Column[]{ table.getPartitioncolumn() };
    	} else {
    		throw new RuntimeException("No partition columns provided for table " + table.getName());
    	}
        return CatalogUtil.getVoltTable(Arrays.asList(cols));
    }


  
}
