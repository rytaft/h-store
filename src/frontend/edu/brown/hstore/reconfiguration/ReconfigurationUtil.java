package edu.brown.hstore.reconfiguration;

import java.util.ArrayList;
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
import org.voltdb.utils.Pair;
import org.voltdb.VoltType;
import org.voltdb.utils.Pair;

import edu.brown.hashing.ReconfigurationPlan;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;

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
        for(Entry<Integer, List<ReconfigurationRange<? extends Comparable<?>>>> entry : plan.getIncoming_ranges().entrySet()){
            Set<Integer> partitionsSendingData = new HashSet<>();
            for(ReconfigurationRange<? extends Comparable<?>> range : entry.getValue()){
                partitionsSendingData.add(range.old_partition);
                migrationPairs.add(new ReconfigurationPair(range.old_partition, range.new_partition));
            }
            LOG.info(String.format("Partition: %s is receiving data from :%s",entry.getKey(),StringUtils.join(partitionsSendingData, ",")));
        }
        LOG.info(String.format("Pairs(%s): %s ",migrationPairs.size(), StringUtils.join(migrationPairs, ",")));

        // Sort the migration pairs to make sure we get the same set of splits each time
        ArrayList<ReconfigurationPair> migrationPairsList = new ArrayList<>();
        migrationPairsList.addAll(migrationPairs);
        Collections.sort(migrationPairsList);
        
        //Limit the number of splits to number of pairs
        if (numberOfSplits > migrationPairsList.size()){
            numberOfSplits = migrationPairsList.size();
            LOG.info("Limiting number of pair splits to " + numberOfSplits);
        }
        
        //Split pairs into groups based on numSplits paramt
        int pairCounter = 0;
        for(ReconfigurationPair mPair : migrationPairsList){
            pairToSplitMapping.put(new Pair(mPair.from, mPair.to), pairCounter % numberOfSplits);
            pairCounter++;
        }

	List<ReconfigurationPlan> splitPlans = new ArrayList<>();
        for(int i = 0; i < numberOfSplits; i++){
            splitPlans.add(new ReconfigurationPlan());
        }

        
        //put ranges into split rangePLans
        for(Entry<Integer, List<ReconfigurationRange<? extends Comparable<?>>>> entry : plan.getIncoming_ranges().entrySet()){
            for(ReconfigurationRange<? extends Comparable<?>> range : entry.getValue()){
                //find which split this range is going into
                Integer splitIndex = pairToSplitMapping.get(new Pair(range.old_partition, range.new_partition));
                //add it
                splitPlans.get(splitIndex).addRange(range);
            }
        }

        //DEBUG
        for (int j = 0; j < numberOfSplits; j++){
            
            Set<Pair<Integer,Integer>> debugSendingData = new HashSet<>();
            for(Entry<Integer, List<ReconfigurationRange<? extends Comparable<?>>>> entry : splitPlans.get(j).getIncoming_ranges().entrySet()){
                for(ReconfigurationRange<? extends Comparable<?>> range : entry.getValue()){
                    debugSendingData.add(new Pair(range.old_partition, range.new_partition));
                }
            }
            LOG.info(String.format("PlanSplit:%s has the pairs(%s): %s", j, debugSendingData.size(), StringUtils.join(debugSendingData,",")));
        }
        
        return splitPlans;
    }
    
    public static VoltTable getExtractVoltTable(ReconfigurationRange range) {
    	ArrayList<VoltType> types = new ArrayList<>();
    	types.add(VoltType.BIGINT);
    	return getExtractVoltTable(range, 1, types);
    }

    public static VoltTable getExtractVoltTable(ReconfigurationRange range, int nCols, List<VoltType> types) {
    	int nSchemaCols = nCols * 3 + 1;
    	ColumnInfo[] extractTableColumns = new ColumnInfo[nSchemaCols];

        extractTableColumns[0] = new ColumnInfo("TABLE_NAME", VoltType.INTEGER);
        for(int i = 0; i+3 < nSchemaCols; i+=3) {
        	VoltType type = types.get(i/3);
        	extractTableColumns[i+1] = new ColumnInfo("KEY_TYPE", VoltType.INTEGER);
            extractTableColumns[i+2] = new ColumnInfo("MIN_INCLUSIVE", type); // range.getVt());
            extractTableColumns[i+3] = new ColumnInfo("MAX_EXCLUSIVE", type); // range.getVt());
        }  
        
        VoltTable vt = new VoltTable(extractTableColumns);
        // vt.addRow(range.table_name,range.getVt().toString(),range.getMin_inclusive(),range.getMax_exclusive());
        
        ArrayList<Object[]> rows = new ArrayList<>();
        Object[] baseRow = new Object[nSchemaCols];
        baseRow[0] = 1;
        
        ReconfigurationRange<Long> r = range;
        for(int i = 0; i+3 < nSchemaCols; i+=3) {
        	if(r != null) {
        		if(r.isSingleRange()) {
        			baseRow[i+1] = 1;
        			baseRow[i+2] = r.getMin_inclusive();
        			baseRow[i+3] = r.getMax_exclusive();
        		}
        		else if(r.getMinList().size() == 1) {
        			baseRow[i+1] = 1;
        			baseRow[i+2] = r.getMinList().get(0);
        			baseRow[i+3] = r.getMaxList().get(0);
    			}
    			else {
        			for (int j = 0; j < r.getMinList().size(); j++) {
	            		Object[] row = baseRow.clone();
	            		row[i+1] = 1;
	            		row[i+2] = r.getMinList().get(j);
	            		row[i+3] = r.getMaxList().get(j);
	            		rows.add(row);
	                }
        			break;
    			}
        		
        		r = r.getSubRange();
        	}
        	else {
        		break;
        	}
        }  
        
        if(rows.size() == 0) {
        	vt.addRow(baseRow);
		}
        
        for(Object[] row : rows) {
        	vt.addRow(row);
        }
        
        
        return vt;
    }

    public static VoltTable getExtractVoltTable(List<Long> minInclusives, List<Long> maxExclusives) {
        if(minInclusives.size()!=maxExclusives.size()){
            throw new RuntimeException("Min inclusive list different size than maxExclusives.");
        }
        ColumnInfo extractTableColumns[] = new ColumnInfo[4];

        extractTableColumns[0] = new ColumnInfo("TABLE_NAME", VoltType.INTEGER);
        extractTableColumns[1] = new ColumnInfo("KEY_TYPE", VoltType.INTEGER);
        extractTableColumns[2] = new ColumnInfo("MIN_INCLUSIVE", VoltType.BIGINT); // range.getVt());
        extractTableColumns[3] = new ColumnInfo("MAX_EXCLUSIVE", VoltType.BIGINT);// range.getVt());

        VoltTable vt = new VoltTable(extractTableColumns);
        for (int i = 0; i < minInclusives.size(); i++) {
            vt.addRow(1, 1, minInclusives.get(i), maxExclusives.get(i));
        }

        return vt;       
    }
    
    public static VoltTable getExtractVoltTable(List<ReconfigurationRange> ranges) {
        ColumnInfo extractTableColumns[] = new ColumnInfo[4];

        extractTableColumns[0] = new ColumnInfo("TABLE_NAME", VoltType.INTEGER);
        extractTableColumns[1] = new ColumnInfo("KEY_TYPE", VoltType.INTEGER);
        extractTableColumns[2] = new ColumnInfo("MIN_INCLUSIVE", VoltType.BIGINT); // range.getVt());
        extractTableColumns[3] = new ColumnInfo("MAX_EXCLUSIVE", VoltType.BIGINT);// range.getVt());

        VoltTable vt = new VoltTable(extractTableColumns);
        // vt.addRow(range.table_name,range.getVt().toString(),range.getMin_inclusive(),range.getMax_exclusive());
        for (ReconfigurationRange range : ranges) {
            if(range.isSingleRange()) {
            	vt.addRow(1, 1, range.getMin_inclusive(), range.getMax_exclusive());
            }
            else {
            	for (int i = 0; i < range.getMinList().size(); i++) {
                    vt.addRow(1, 1, range.getMinList().get(i), range.getMaxList().get(i));
                }
            }
        }

        return vt;
    }
}
