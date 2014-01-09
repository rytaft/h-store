package org.qcri.ehstore.placement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Plan {
	
	// the TreeMap is a range. The key is the beginning of interval, the value is the end.
	private Map<Integer, TreeMap<Integer, Integer>> partitionToRanges = new HashMap<Integer, TreeMap<Integer, Integer>>();
	public class Range{
		Integer from;
		Integer to;
	}
	
	// creates empty plan
	public Plan (Collection<Integer> partitions){
		TreeMap<Integer, Integer> emptyRange = new TreeMap<Integer,Integer>();
		for (Integer partition: partitions){
			partitionToRanges.put(partition, emptyRange);
		}
	}
	
	public Plan (String filename){
		fromJSON(filename);
	}

	public Plan() {
		// TODO Only for testing
	}

	public void addRange(Integer partition, Integer from, Integer to){
		TreeMap<Integer, Integer> ranges = partitionToRanges.get(partition);
		Map.Entry<Integer, Integer> precedingFrom = ranges.floorEntry(from);
		Map.Entry<Integer, Integer> precedingTo = ranges.floorEntry(to);
		if (precedingFrom == null){
			// from is smaller than any previous range
			if (precedingTo == null){
				// no range intersecting with this range, create a new range
				ranges.put(from,to);
			}
			else{
				// merge the two ranges
				// first remove all ranges before precedingTo
				ranges.headMap(precedingTo.getKey(),true).clear();
				ranges.put(from, Math.max(to,precedingTo.getValue()));
			}
		}
		else{
			if(precedingTo.equals(precedingFrom) && from < precedingFrom.getValue()){
				// no range intersecting with this range, create a new range
				ranges.put(from,to);
			}
			else{
				// remove all ranges intersecting with (from,to) except the one preceding it 
				ranges.subMap(precedingFrom.getKey(),false,precedingTo.getKey(),true).clear();
				// merge
				precedingFrom.setValue(Math.max(precedingTo.getValue(), to));
			}
		}
	}
	
	public boolean removeRange(Integer partition, Integer from){
		return (partitionToRanges.get(partition).remove(from) == null);
	}
	
	public Range getRangeValue(Integer partition, Integer value){
		Map.Entry<Integer,Integer> precedingRange = partitionToRanges.get(partition).floorEntry(value);
		if(precedingRange == null) return null;
		if(precedingRange.getValue() >= value){
			Range res = new Range();
			res.from = precedingRange.getKey();
			res.to = precedingRange.getValue();
			return res;
		}
		else return null;
	}
	
	public List<Range> getAllRanges(Integer partition){
		List<Range> res = new ArrayList<Range>();
		TreeMap<Integer, Integer> ranges = partitionToRanges.get(partition);
		for(Map.Entry<Integer, Integer> range : ranges.entrySet()){
			Range tmp = new Range();
			tmp.from = range.getKey();
			tmp.to = range.getValue();
			res.add(tmp);
		}
		return res;
	}

	public Map<Integer, List<Range>> getAllRanges(){
		Map<Integer, List<Range>> res = new HashMap<Integer, List<Range>>();
		for(Integer partition: partitionToRanges.keySet()){
			res.put(partition,getAllRanges(partition));
		}
		return res;
	}
	
	private void fromJSON(String filename){
		// TODO this method must output the JSON file in the correct format
	}
	
	public void toJSON (String filename){
		// TODO this method must output the JSON file in the correct format
	}
}
