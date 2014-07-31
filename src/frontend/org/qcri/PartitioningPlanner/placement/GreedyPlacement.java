package org.qcri.PartitioningPlanner.placement;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.voltdb.utils.Pair;
import org.qcri.PartitioningPlanner.placement.Plan;
import org.voltdb.CatalogContext;


public class GreedyPlacement extends Placement {
	
	Long coldPartitionWidth = 100000L; // redistribute cold tuples in chunks of 100000 if shrinking
	
	public GreedyPlacement(){
		
	}
	
	
	// hotTuples: tupleId --> access count
	// siteLoads: partitionId --> total access count
	public Plan computePlan(ArrayList<Map<Long, Pair<Long,Integer> >> hotTuplesList, Map<Integer, Pair<Long,Integer>> partitionTotals, String planFilename, int partitionCount, int timeLimit, CatalogContext catalogContext){
		
		Integer dstPartition = -1;
		Long totalAccesses = 0L;
		Long meanAccesses;
		Long hotTupleCount = 0L;
		
		Plan aPlan = new Plan(planFilename);

		for(Integer i : partitionTotals.keySet()) {
			totalAccesses = totalAccesses + partitionTotals.get(i).getFirst();			
		}
		for(int i = 0; i < partitionCount; ++i) {
		    if(partitionTotals.get(i) == null) {
		    	partitionTotals.put(i, new Pair<Long, Integer>(0L, 0));
		    }
		}
		
		// copy hot tuples list
		ArrayList<Map<Long, Pair<Long,Integer> >> hotTuplesListCopy = new ArrayList<Map<Long, Pair<Long,Integer> >>();
		hotTuplesListCopy.addAll(hotTuplesList);
		
		meanAccesses = totalAccesses / partitionCount;

		System.out.println("Mean access count: " + meanAccesses);
		
		for(Map<Long, Pair<Long,Integer> > hotTuples : hotTuplesList) {
			hotTupleCount = hotTupleCount + hotTuples.size();
		}

		System.out.println("Received " + hotTupleCount + " hot tuples.");
		
		for(Long i = 0L; i < hotTupleCount; ++i) {
			getHottestTuple(hotTuplesList);
			//System.out.println("Processing hot tuple id " + _hotTupleId + " with access count " + _hotAccessCount);

			if(partitionTotals.get(_srcPartition).getFirst() > meanAccesses || _srcPartition >= partitionCount) {
					dstPartition = getMostUnderloadedPartitionId(partitionTotals, partitionCount);
					if(dstPartition != _srcPartition) {
					        //System.out.println(" sending it to " + dstPartition);
						partitionTotals.put(_srcPartition, new Pair<Long, Integer>(partitionTotals.get(_srcPartition).getFirst()  - _hotAccessCount, 
								partitionTotals.get(_srcPartition).getSecond()  - _hotSize));
						partitionTotals.put(dstPartition, new Pair<Long, Integer>(partitionTotals.get(dstPartition).getFirst()  + _hotAccessCount, 
								partitionTotals.get(dstPartition).getSecond()  + _hotSize));
						aPlan.removeTupleId(_srcPartition, _hotTupleId);
						if(!aPlan.hasPartition(dstPartition)) {
							aPlan.addPartition(dstPartition);
						}
						aPlan.addRange(dstPartition, _hotTupleId, _hotTupleId);
					}
				}
			hotTuplesList.get(_srcPartition).remove(_hotTupleId);

		}

		// If we are shrinking the number of partitions, use the first fit approach 
		// to place the cold tuples from the deleted partitions
		for(Integer i : aPlan.getAllRanges().keySet()) { // foreach partition
			if(i.intValue() >= partitionCount) { // in case of shrinking number of partitions
				// VOTER HACK: we want each partition slice to contain ~1000 tuples, but we don't know how many tuples
				// are in a range
				Double tuplesPerKey = (double) partitionTotals.get(i).getSecond() / Plan.getRangeListWidth(aPlan.getAllRanges(i));
				List<List<Plan.Range>> partitionSlices = aPlan.getRangeSlices(i,  (long) (coldPartitionWidth / tuplesPerKey));
				if(partitionSlices.size() > 0) {
				        
					Double tupleWeight = (double) partitionTotals.get(i).getFirst() / partitionTotals.get(i).getSecond(); // per tuple - VOTER HACK
					
					for(List<Plan.Range> slice : partitionSlices) {  // for each slice

						Boolean placed = false;
						// VOTER HACK
						Integer sliceSize = (int) (Plan.getRangeListWidth(slice) * tuplesPerKey);
						Long newWeight = (long) (tupleWeight *  ((double) sliceSize));
						
						for(Integer k : partitionTotals.keySet()) {
							if(partitionTotals.get(k).getFirst() + newWeight <= meanAccesses) {
								dstPartition = k;
								placed = true;
								break;
							}
						}
						if(!placed) {
							dstPartition = getMostUnderloadedPartitionId(partitionTotals, partitionCount);
						}
						for(Plan.Range r : slice) { 
							if(!aPlan.hasPartition(dstPartition)) {
								aPlan.addPartition(dstPartition);
							}
							aPlan.removeRange(i, r.from);
							aPlan.addRange(dstPartition, r.from, r.to);
						}
						partitionTotals.put(dstPartition, new Pair<Long, Integer>(partitionTotals.get(dstPartition).getFirst() + newWeight, 
								partitionTotals.get(dstPartition).getSecond() + sliceSize));

					} // end for each slice
				}
			} // end in case of shrinking number of partitions
		} // end for each partition

		if(!catalogContext.jarPath.getName().contains("tpcc")) {
			aPlan = demoteTuples(hotTuplesListCopy, aPlan);
		}
		removeEmptyPartitions(aPlan);
		return aPlan;
		
	}
	

}
