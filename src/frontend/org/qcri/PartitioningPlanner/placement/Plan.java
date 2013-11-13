package org.qcri.PartitioningPlanner.placement;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.json.JSONException;
import org.json.JSONObject;
import edu.brown.utils.FileUtil;
import java.util.StringTokenizer;
import java.io.BufferedWriter;
import java.io.FileWriter;

public class Plan {
	
	public static final String PLANNED_PARTITIONS = "partition_plans";
	// the TreeMap is a range. The key is the beginning of interval, the value is the end.
	private Map<Integer, TreeMap<Long, Long>> partitionToRanges = new HashMap<Integer, TreeMap<Long, Long>>();
	public class Range{
		Long from;
		Long to;
		

		public String toString() {
			String rangeStr = new String();
			if(from == to) {
				rangeStr = from.toString();
			}
			else{
				rangeStr = from.toString() + "-" + to.toString();
			}
			return rangeStr;
		}
			

	}
	
	// creates empty plan
	public Plan (Collection<Integer> partitions){
		TreeMap<Long, Long> emptyRange = new TreeMap<Long,Long>();
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

	public void addPartition(Integer partitionId) {
		TreeMap<Long, Long> emptyRange = new TreeMap<Long,Long>();
		partitionToRanges.put(partitionId, emptyRange);

	}
	
	public void addRange(Integer partition, Long from, Long to){
		TreeMap<Long, Long> ranges = partitionToRanges.get(partition);
		// find plans that intersect or are adjacent to the new range
		Map.Entry<Long, Long> precedingFrom = ranges.floorEntry(from - 1);
		Map.Entry<Long, Long> precedingTo = ranges.floorEntry(to + 1);

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
			if(precedingTo.equals(precedingFrom) && from > precedingFrom.getValue()){
				// no range intersecting with this range, create a new range
				ranges.put(from,to);
			}
			else{
				// remove all ranges intersecting with (from,to) except the one preceding it 
				ranges.subMap(precedingFrom.getKey(),false,precedingTo.getKey(),true).clear();
				// merge
				Long lowerBound = precedingFrom.getKey();
				Long upperBound = Math.max(precedingTo.getValue(), to);
				
				// object is immutable - replace it
				removeRange(partition, lowerBound);
				ranges.put(lowerBound, upperBound);
				
			}
		}
	}
	
	public boolean removeRange(Integer partition, Long from){
		return (partitionToRanges.get(partition).remove(from) == null);
	}
	
	public boolean removeTupleId(Integer partition, Long tupleId) {
		Map.Entry<Long,Long> precedingRange = partitionToRanges.get(partition).floorEntry(tupleId);
		if(precedingRange == null) return false;
		if(precedingRange.getValue() > tupleId && precedingRange.getKey() < tupleId){ // split into two ranges
			//System.out.println("Splitting up range " + precedingRange.getKey() + "-" + precedingRange.getValue() + " at " + tupleId);
			Long upperBound = precedingRange.getValue();
			Long lowerBound = precedingRange.getKey();
			partitionToRanges.get(partition).remove(lowerBound);
			partitionToRanges.get(partition).put(lowerBound, tupleId - 1);
			partitionToRanges.get(partition).put(tupleId + 1, upperBound);
		}
		// range of one
		else if(precedingRange.getKey() == precedingRange.getValue()) {
			partitionToRanges.remove(tupleId);
		}
		else if(precedingRange.getValue() == tupleId) {
			Long lowerBound = precedingRange.getKey();
			partitionToRanges.get(partition).remove(lowerBound);
			partitionToRanges.get(partition).put(lowerBound, tupleId - 1);

		}
		else if(precedingRange.getKey() == tupleId) {
			Long upperBound = precedingRange.getValue();
			partitionToRanges.remove(tupleId);
			partitionToRanges.get(partition).put(tupleId + 1, upperBound);
		}
			
		return true;
	}
	
	
	Integer getTuplePartition(Long tupleId) {
		for(Integer partition : partitionToRanges.keySet()) {
			if(getRangeValue(partition, tupleId) != null) {
				return partition;
			}
		}
		return -1;
	}
	
	public String printPartition(Integer partition) {
		String output = new String();
		Boolean first = true;
		
		TreeMap<Long, Long> ranges = partitionToRanges.get(partition);
		for(Map.Entry<Long, Long> range : ranges.entrySet()){
			if(range.getKey() == range.getValue()) {
				if(first) {
					output = range.getKey().toString();
					first = false;
				}
				else {
				output = output + "," + range.getKey();
				}
			}
			else {
				if(first) {
 				 output = range.getKey() + "-" + range.getValue();
 				 first = false;
				}
				else {
					output = output + "," + range.getKey() + "-" + range.getValue();
				}
			}
		
			
		}

		return output;
		
	}
	
	
	private Range  parseRange(String src) {
		Long value;
		Range parsed = new Range();
		
		if(src.contains("-")) {
			StringTokenizer inner = new StringTokenizer(src, "-");
			String lhs = inner.nextToken();
			String rhs = inner.nextToken();
			parsed.from = Long.parseLong(lhs);
			parsed.to = Long.parseLong(rhs);
		 }
		 else {
			 value = Long.parseLong(src);
			 parsed.from = parsed.to = value;
		 }
		System.out.println("Parsed internal " + parsed.toString());
		return parsed;

	
	}
	
	private TreeMap<Long, Long> parseRanges(String srcRanges) {
		TreeMap<Long, Long> ranges = new TreeMap<Long, Long>();
		StringTokenizer st = new StringTokenizer(srcRanges, ",");
		Range parsedRange;
		
		// just one
		if(srcRanges.contains(",") == false && srcRanges.length() > 0) {
		
			parsedRange = parseRange(srcRanges);
			ranges.put(parsedRange.from, parsedRange.to);
		}
		
		while (st.hasMoreTokens()) { 
			 String rangeStr = st.nextToken();
			 parsedRange = parseRange(rangeStr);
			 ranges.put(parsedRange.from, parsedRange.to);
		 }
		
		return ranges;
		
	}
	
    public void printPlan() {
    	
    	for(Integer partition : partitionToRanges.keySet()) {
			System.out.print("Partition " + partition + ": " + printPartition(partition));
			System.out.println("");
	
    	}
    }
    	

	

	
	public Range getRangeValue(Integer partition, Long value){
		Map.Entry<Long,Long> precedingRange = partitionToRanges.get(partition).floorEntry(value);
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
		TreeMap<Long, Long> ranges = partitionToRanges.get(partition);
		for(Map.Entry<Long, Long> range : ranges.entrySet()){
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
	
	public Long getTupleCount(Integer partition) {
		TreeMap<Long, Long> ranges = partitionToRanges.get(partition);
		Long sum = 0L;
		for(Long k : ranges.keySet()) {
			sum += ranges.get(k) - k + 1; // inclusive
		}
		return sum;
	}

	static Long getRangeWidth(Range r) {
		return r.to - r.from + 1; // inclusive
	}
	
	static String rangeSliceToString(List<Range> slice) {
		String printed = new String();
		
		if(slice.size() > 0) {
			printed = slice.get(0).toString();
			for(Integer i = 1; i < slice.size(); ++i) {
				printed = printed + "," + slice.get(i).toString();
			}
		}
		return printed;
	}
	
	static void printRangeSlices(List<List<Range>> slices) {
		for(Integer i = 0; i < slices.size(); ++i) {
			System.out.println("Slice " + i + ": " + rangeSliceToString(slices.get(i)));
		}
	}
	
	static Long getRangeListWidth(List<Range> ranges) {
		Long sum = 0L;
		
		for(Range r : ranges) {
			sum = sum + getRangeWidth(r);
		}
		return sum;
	}
	// for algos such as the first fit, bin packer, subdivide cold tuples
	// into sets of equal size from pre-existing ranges
	List<List<Range>> getRangeSlices(Integer partition, Long sliceWidth) {
		TreeMap<Long, Long> ranges = partitionToRanges.get(partition);
		List<List<Range>> slices = new ArrayList<List<Range>>();

		Long accumulatedSum = 0L;
		List<Range> partialList = new ArrayList<Range>();
		Long rangeLength = 0L;
		
		for(Long i : ranges.keySet()) {
			rangeLength = ranges.get(i) - i + 1; // inclusive
			if(accumulatedSum + rangeLength < sliceWidth) {
				Range tmp = new Range();
				tmp.from = i;
				tmp.to = ranges.get(i);
				partialList.add(tmp);
				accumulatedSum = accumulatedSum + rangeLength;
			}
			else if(accumulatedSum + rangeLength == sliceWidth) {
				// emit
				slices.add(partialList);
				partialList = new ArrayList<Range>();
				accumulatedSum = 0L;
			}
			// selection intersects range
			else {
				Range tmp1 = new Range();
				tmp1.from = i;
				tmp1.to = i + sliceWidth - accumulatedSum - 1; // fill in the remainder
				
				partialList.add(tmp1);
				slices.add(partialList);

				
				partialList = new ArrayList<Range>();
				accumulatedSum = 0L;
				
				Range tmp2 = new Range();
				tmp2.from = tmp1.to + 1;
				tmp2.to = ranges.get(i);
				// if it spans multiple slices, unwind it
				while(getRangeWidth(tmp2) > sliceWidth) {
					tmp1 = new Range();
					tmp1.from = tmp2.from;
					tmp1.to = tmp2.from + sliceWidth - 1;
					partialList.add(tmp1);
				
					slices.add(partialList);
					
					partialList = new ArrayList<Range>();
					tmp2.from = tmp1.to + 1;
				}
				
				if(getRangeWidth(tmp2) == sliceWidth) {
					slices.add(partialList);
					partialList = new ArrayList<Range>();					
				}
				else {
					accumulatedSum = getRangeWidth(tmp2);
					partialList.add(tmp2);
				}
			}
		}
	
		if(!partialList.isEmpty()) {
			slices.add(partialList);
		}
		
		return slices;
		
	}
	
	private void fromJSON(String filename)  {
		partitionToRanges.clear();
		readInFile(filename);
	}
	
	public void toJSON (String filename)  {
		JSONObject srcData = new JSONObject(), planList, newPlan;
		String oldPlan = new String();
		System.out.println("Planner writing to " + filename);
		try {
			oldPlan = FileUtil.readFile(filename);
		} catch(Exception e) {
			System.out.println("Failed to read in " + filename);
			}
		
		// begin JSON block
		try {
			if(oldPlan.length() > 0) {
				srcData = new JSONObject(oldPlan);					
			}
			
			if(srcData.has(PLANNED_PARTITIONS)) {
				planList = srcData.getJSONObject(PLANNED_PARTITIONS);
			}
			else {
				planList = new JSONObject();
				srcData.put(PLANNED_PARTITIONS, planList);	
			}

			
			String[]  partitionKeys = JSONObject.getNames(planList);
			Integer planNo = 1;
			if(partitionKeys != null) {
				planNo = partitionKeys.length + 1;
			}
		
			System.out.println("Constructing plan #" + planNo + " from " + oldPlan);
		
			newPlan = constructNewJSON();
			planList.put(planNo.toString(), newPlan);

			partitionKeys = JSONObject.getNames(srcData.getJSONObject(PLANNED_PARTITIONS));
			System.out.println("Concluding with " + partitionKeys.length + " entries.");
		
		} catch(JSONException f) {
			System.out.println("Init of JSONObject in toJSON failed!");
		}

		try {
    		BufferedWriter output = new BufferedWriter(new FileWriter(filename));    	 
    		output.write(srcData.toString(2));
    		output.close();
    	}
    	catch (Exception e) {
    		System.out.println("Failed to write partitioning plan to " + filename);
    		return;
    	}
	}
	
	// build a JSON object using the current contents of this Plan object
	private JSONObject constructNewJSON()  throws JSONException {
		JSONObject jsonPlan = new JSONObject();
		JSONObject tableNameObject = new JSONObject();
		JSONObject partitionDelimiter = new JSONObject();
		JSONObject tableObject = new JSONObject();
		
		jsonPlan.put("tables", tableNameObject);
		tableNameObject.put("usertable", partitionDelimiter);
		partitionDelimiter.put("partitions", tableObject);
		
		
    	for(Integer partition : partitionToRanges.keySet()) {
    		tableObject.put(partition.toString(), printPartition(partition));
    	}
    	
    	return jsonPlan;

	}
	
	private JSONObject traverseLevel(JSONObject srcData, String key) {
		try {
			System.out.println("Traversing JSON key " + key);
			JSONObject child = srcData.getJSONObject(key);
			return child;
			//srcData = child;
			//String[] keys = JSONObject.getNames(srcData);
			//System.out.println("Now srcData has " + keys.length + " keys first one is " + keys[0] );
		}
		catch (JSONException e) {
			System.out.println("Failed to recurse down JSON tree for key " + key);
			return null;
		}
	}
	
	private JSONObject traverseLevelSingle(JSONObject srcData) {
		String[] keys = JSONObject.getNames(srcData);
		return traverseLevel(srcData, keys[0]);
	}

	// read in the last plan
	public void readInFile(String filename) {
		JSONObject srcData;
		String inputData = FileUtil.readFile(filename);
		
		System.out.println("Working from " + inputData);
		try {
			srcData = new JSONObject(inputData);	
		} catch(JSONException e) {
			System.out.println("Failed to read in " + filename);
			return;
		}
		
		if(!srcData.has(PLANNED_PARTITIONS)) {
			return;
		}
		
		// traverse "partition_plans" object
		srcData = traverseLevelSingle(srcData);
		
		
		// have a list of plans - get the last one
		String[] keys = JSONObject.getNames(srcData);
		srcData = traverseLevel(srcData, keys[keys.length - 1]);
		
		
		for(Integer i = 0; i < 3; ++i) {
			srcData = traverseLevelSingle(srcData);			
		}

		String[]  partitionKeys = JSONObject.getNames(srcData);
		for(Integer i = 0; i < partitionKeys.length; ++i) {
			Integer partitionNo = Integer.parseInt(partitionKeys[i]);
			String partitionRanges = new String();
			try {
				partitionRanges = srcData.getString(partitionKeys[i]);
			} catch(JSONException e) {
				System.out.println("Failed to parse a partition range " + partitionNo);
				return;
			}
			partitionToRanges.put(partitionNo, parseRanges(partitionRanges));
		}
		
	
	}
}
