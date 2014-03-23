package org.qcri.PartitioningPlanner.placement;


import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

import org.voltdb.utils.Pair;
import org.qcri.PartitioningPlanner.placement.Plan;


public class FirstFitPlacement extends Placement {
	
	Long coldPartitionWidth = 100000L; // redistribute cold tuples in chunks of 100000
	
	public FirstFitPlacement(){
		
	}
	
	// hotTuples: tupleId --> access count
	// siteLoads: partitionId --> total access count
	public Plan computePlan(ArrayList<Map<Long, Pair<Long,Integer> >> hotTuplesList, Map<Integer, Pair<Long,Integer>> partitionTotals, String planFilename, int partitionCount, int timeLimit){
		

		Integer dstPartition = -1;
		Long totalAccesses = 0L;
		Long targetCapacity;
		Map<Integer, Pair<Long,Integer>> oldLoad = new HashMap<Integer, Pair<Long,Integer>> ();
		Plan aPlan = new Plan(planFilename);
		Plan newPlan = new Plan();
		Long hotTupleCount = 0L;
		

		for(Integer i : partitionTotals.keySet()) {
			totalAccesses = totalAccesses + partitionTotals.get(i).getFirst();			
			oldLoad.put(i,  partitionTotals.get(i));
		}

		for(int i = 0; i < partitionCount; ++i) {
		    // zero out the load for a plan
		    partitionTotals.put(i, new Pair<Long, Integer>(0L, 0));
		}
		
		// copy hot tuples list
		ArrayList<Map<Long, Pair<Long,Integer> >> hotTuplesListCopy = new ArrayList<Map<Long, Pair<Long,Integer> >>();
		hotTuplesListCopy.addAll(hotTuplesList);
				
		targetCapacity = totalAccesses / partitionCount;	
		//System.out.println("Target capacity " + targetCapacity);
		
		Map<Integer, List<Plan.Range>> ranges = aPlan.getAllRanges();
		for(Integer i : ranges.keySet()) {
			newPlan.addPartition(i);
		}
		
		for(Map<Long, Pair<Long,Integer> > hotTuples : hotTuplesList) {
			hotTupleCount = hotTupleCount + hotTuples.size();
		}
		
		// pack the hot tuples first
		for(Integer i = 0; i < hotTupleCount; ++i) {
				getHottestTuple(hotTuplesList);

				Boolean placed = false;
				for(Integer j : partitionTotals.keySet()) {
					if(partitionTotals.get(j).getFirst() + _hotAccessCount <= targetCapacity) {
						dstPartition = j;
						placed = true;
						break;
					}
			
				} // end inner-for
			
				if(!placed) {
					dstPartition = getMostUnderloadedPartitionId(partitionTotals, partitionCount);
				}		
			
				//System.out.println("Processing hot tuple id " + _hotTupleId + " with access count " + _hotAccessCount + " sending it to " + dstPartition);
				int size = _hotSize;
				if (size > 1000) {
					size = _hotAccessCount.intValue();
				}
				partitionTotals.put(dstPartition, new Pair<Long, Integer>(partitionTotals.get(dstPartition).getFirst()  + _hotAccessCount,
						partitionTotals.get(dstPartition).getSecond()  + size));
				oldLoad.put(_srcPartition, new Pair<Long, Integer>(oldLoad.get(_srcPartition).getFirst() - _hotAccessCount,
						oldLoad.get(_srcPartition).getSecond() - size));
				hotTuplesList.get(_srcPartition).remove(_hotTupleId);
				aPlan.removeTupleId(_srcPartition, _hotTupleId);
				if(!newPlan.hasPartition(dstPartition)) {
					newPlan.addPartition(dstPartition);
				}
				newPlan.addRange(dstPartition, _hotTupleId, _hotTupleId);
			} // end outer-for
		
		
		for(Integer i : aPlan.getAllRanges().keySet()) { // foreach partition
			// VOTER HACK: we want each partition slice to contain ~1000 tuples, but we don't know how many tuples
			// are in a range
			Double tuplesPerKey = (double) oldLoad.get(i).getSecond() / Plan.getRangeListWidth(aPlan.getAllRanges(i));
			List<List<Plan.Range>> partitionSlices = aPlan.getRangeSlices(i,  (long) (coldPartitionWidth / tuplesPerKey));
			if(partitionSlices.size() > 0) {
				Double tupleWeight = (double) oldLoad.get(i).getFirst() / oldLoad.get(i).getSecond(); // per tuple - VOTER HACK
				
				for(List<Plan.Range> slice : partitionSlices) {  // for each slice

						Boolean placed = false;
						// VOTER HACK
						Integer sliceSize = (int) (Plan.getRangeListWidth(slice) * tuplesPerKey);
						Long newWeight = (long) (tupleWeight *  ((double) sliceSize));
						
						for(Integer k : partitionTotals.keySet()) {
							if(partitionTotals.get(k).getFirst() + newWeight <= targetCapacity) {
								dstPartition = k;
								placed = true;
								break;
							}
						}
						if(!placed) {
							dstPartition = getMostUnderloadedPartitionId(partitionTotals, partitionCount);
						}
						for(Plan.Range r : slice) { 
							if(!newPlan.hasPartition(dstPartition)) {
								newPlan.addPartition(dstPartition);
							}
							newPlan.addRange(dstPartition, r.from, r.to);
						}
						partitionTotals.put(dstPartition, new Pair<Long, Integer>(partitionTotals.get(dstPartition).getFirst() + newWeight,
								partitionTotals.get(dstPartition).getSecond() + sliceSize));

				} // end destination partition selection 

					

				} // end for each slice
				
			
			
			
		} // end for each partition

		newPlan = demoteTuples(hotTuplesListCopy, newPlan);		
		removeEmptyPartitions(newPlan);
		return newPlan;

	}
	

}
