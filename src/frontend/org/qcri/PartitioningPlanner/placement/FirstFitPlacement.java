package org.qcri.PartitioningPlanner.placement;


import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

import org.qcri.PartitioningPlanner.placement.Plan;


public class FirstFitPlacement extends Placement {
	
	Long coldPartitionWidth = 1000L; // redistribute cold tuples in chunks of 1000
	
	public FirstFitPlacement(){
		
	}
	
	static Integer getMostUnderloadedPartitionId(Map<Integer, Long> partitionTotals) {
		Long minTotal = java.lang.Long.MAX_VALUE; 
		Integer minPartition = -1;

		for(Integer i : partitionTotals.keySet()) {
			if(partitionTotals.get(i) < minTotal) {
				minPartition = i;
				minTotal = partitionTotals.get(i);
			}
			
		}
		
		return minPartition;
	}
	
	// hotTuples: tupleId --> access count
	// siteLoads: partitionId --> total access count
	public Plan computePlan(ArrayList<Map<Long, Long>> hotTuplesList, Map<Integer, Long> partitionTotals, Plan aPlan){
		

		Integer srcPartition = 0, dstPartition = -1;
		Long totalAccesses = 0L;
		Long targetCapacity;
		Map<Integer, Long> oldLoad = new HashMap<Integer, Long> ();
		Plan newPlan = new Plan();
		

		for(Integer i : partitionTotals.keySet()) {
			totalAccesses = totalAccesses + partitionTotals.get(i);			
			oldLoad.put(i,  partitionTotals.get(i));
			// zero out the load for a plan
			partitionTotals.put(i, 0L);
		}

		
		targetCapacity = totalAccesses / partitionTotals.size();		

		Map<Integer, List<Plan.Range>> ranges = aPlan.getAllRanges();
		for(Integer i : ranges.keySet()) {
			newPlan.addPartition(i);
		}
		
		// pack the hot tuples first
		for(Map<Long, Long> hotTuples : hotTuplesList) {
			
			for(Long i : hotTuples.keySet()) {
				Boolean placed = false;
				for(Integer j : partitionTotals.keySet()) {
					if(partitionTotals.get(j) + hotTuples.get(i) <= targetCapacity) {
						dstPartition = j;
						placed = true;
						break;
					}
			
				} // end inner-for
			
				if(!placed) {
					dstPartition = getMostUnderloadedPartitionId(partitionTotals);
				}		
			
				partitionTotals.put(dstPartition,partitionTotals.get(dstPartition)  + hotTuples.get(i));
				oldLoad.put(srcPartition, oldLoad.get(srcPartition) - hotTuples.get(i));
				aPlan.removeTupleId(srcPartition, i);
				newPlan.addRange(dstPartition, i, i);
			} // end outer-for
			++srcPartition;
		} // end partition-for
		
		
		
		for(Integer i : aPlan.getAllRanges().keySet()) { // foreach partition
			List<List<Plan.Range>> partitionSlices = aPlan.getRangeSlices(i,  coldPartitionWidth);
			if(partitionSlices.size() > 0) {
				Double tupleWeight = ((double) oldLoad.get(i)) / aPlan.getTupleCount(i); // per tuple

				for(List<Plan.Range> slice : partitionSlices) {  // for each slice

						Boolean placed = false;
						Integer newWeight = (int) (tupleWeight *  ((double) Plan.getRangeListWidth(slice)));

						for(Integer k : partitionTotals.keySet()) {
							if(partitionTotals.get(k) + newWeight <= targetCapacity) {
								dstPartition = k;
								placed = true;
								break;
							}
						}
						if(!placed) {
							dstPartition = getMostUnderloadedPartitionId(partitionTotals);
						}
						for(Plan.Range r : slice) { 
							newPlan.addRange(dstPartition, r.from, r.to);
						}
						partitionTotals.put(dstPartition, partitionTotals.get(dstPartition) + newWeight);

				} // end destination partition selection 

					

				} // end for each slice
				
			
			
			
		} // end for each partition
		
		return newPlan;

	}
	

}
