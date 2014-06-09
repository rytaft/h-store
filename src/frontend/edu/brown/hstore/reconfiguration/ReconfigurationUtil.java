package edu.brown.hstore.reconfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
            splitPlans.add(new ReconfigurationPlan());
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
        
        int extraSplitsPerPlan = extraSplits / splitPlans.size();
        if(extraSplitsPerPlan > 0) {
        	List<ReconfigurationPlan> splitPlansAgain = new ArrayList<>();
        	for(ReconfigurationPlan splitPlan : splitPlans) {
        		splitPlansAgain.addAll(fineGrainedSplitReconfigurationPlan(splitPlan, extraSplitsPerPlan));
        	}
        	return splitPlansAgain;
        }
        
        return splitPlans;
    }
    
    public static List<ReconfigurationPlan> fineGrainedSplitReconfigurationPlan(ReconfigurationPlan plan, int numberOfSplits){
    	
    	List<ReconfigurationPlan> splitPlans = new ArrayList<>();
    	
    	// split up the ranges by key schema so we only compare ranges with the same key schema
    	HashMap<VoltType[], List<ReconfigurationRange>> rangeMap = new HashMap<>();
    	for(List<ReconfigurationRange> ranges : plan.getIncoming_ranges().values()){
    		for(ReconfigurationRange range : ranges) {
    			VoltType[] types = new VoltType[range.getKeySchema().getColumnCount()];
    			for(int i = 0; i < types.length; i++) {
    				types[i] = range.getKeySchema().getColumnType(i);
    			}
    			if(rangeMap.get(types) == null) {
    				rangeMap.put(types, new ArrayList<ReconfigurationRange>());
    			}
    			rangeMap.get(types).add(range);
    		}
        }
    	
    	// sort all the ranges with the same key schema
    	int numRanges = 0;
    	for(Entry<VoltType[],List<ReconfigurationRange>> entry : rangeMap.entrySet()) {
    		rangeMap.put(entry.getKey(), CollectionUtil.sort(entry.getValue()));
    		numRanges += entry.getValue().size();
    	}

    	if(numberOfSplits > numRanges) {
    		numberOfSplits = numRanges;
    		LOG.info("Limiting number of range splits to " + numberOfSplits);
    	}

    	// split up the ranges into numberOfSplits chunks
    	int rangesPerSplit = (int) Math.ceil(((double) numRanges)/numberOfSplits);
    	int i = 0;
    	ReconfigurationPlan newPlan = new ReconfigurationPlan();
    	for(List<ReconfigurationRange> ranges : rangeMap.values()) {
    		for(ReconfigurationRange range : ranges) {
    			newPlan.addRange(range);
    			i++;
    			if(i == rangesPerSplit) {
    				LOG.info("Adding a new reconfiguration plan: " + newPlan.getIncoming_ranges().values().toString());
    				splitPlans.add(newPlan);
    				i = 0;
    				newPlan = new ReconfigurationPlan();
    			}
    		}
    	}
    	if(i > 0) {
    		splitPlans.add(newPlan);
    	}
        
        return splitPlans;
    }
    
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
    
    public static VoltTableComparator getComparator(VoltTable vt) {
    	ArrayList<Pair<Integer, SortDirectionType>> sortCol = new ArrayList<Pair<Integer, SortDirectionType>>();
    	for(int i = 0; i < vt.getColumnCount(); i++) {
    		sortCol.add(Pair.of(i, SortDirectionType.ASC));
    	}
    	return createComparator(vt, sortCol, sortCol.get(0));
    }
    
    private static VoltTableComparator createComparator(VoltTable vt, ArrayList<Pair<Integer, SortDirectionType>> sortCol, Pair<Integer, SortDirectionType>...pairs ) {
    	return new VoltTableComparator(vt, sortCol.toArray(pairs));
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
