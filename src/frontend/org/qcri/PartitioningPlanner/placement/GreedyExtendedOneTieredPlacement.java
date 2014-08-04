package org.qcri.PartitioningPlanner.placement;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.qcri.PartitioningPlanner.placement.Plan;


public class GreedyExtendedOneTieredPlacement extends Placement {
	
	Long coldPartitionWidth = 100000L; // redistribute cold tuples in chunks of 100000
	
	public GreedyExtendedOneTieredPlacement(){
		
	}
	
	
	// hotTuples: tupleId --> access count
	// siteLoads: partitionId --> total access count
	public Plan computePlan(ArrayList<Map<Long, Long>> hotTuplesList, Map<Integer, Long> partitionTotals, String planFilename, int partitionCount, int timeLimit){
		
		// ignore the hotTuplesList!
		
		Integer dstPartition = -1;
		Long totalAccesses = 0L;
		Long meanAccesses;
		
		Plan aPlan = new Plan(planFilename);

		for(int i = 0; i < partitionCount; ++i) {
		    if(partitionTotals.get(i) == null) {
			partitionTotals.put(i, 0L);
		    }
		}
		
		// copy partitionTotals into oldLoad
		Map<Integer, Long> oldLoad = new HashMap<Integer, Long> ();
		for(Integer i : partitionTotals.keySet()) {
			totalAccesses += partitionTotals.get(i);
			oldLoad.put(i,  partitionTotals.get(i));
		}
		
		// copy aPlan into oldPlan
		Plan oldPlan = new Plan();
		Map<Integer, List<Plan.Range>> ranges = aPlan.getAllRanges();
		for(Integer i : ranges.keySet()) {
			List<Plan.Range> partitionRanges = ranges.get(i);
			oldPlan.addPartition(i);
			for(Plan.Range range : partitionRanges) {
				oldPlan.addRange(i, range.from, range.to);
			}
		}
				
		meanAccesses = totalAccesses / partitionCount;

		System.out.println("Mean access count: " + meanAccesses);
				
		// place the cold tuples from the overloaded or deleted partitions
		for(Integer i : oldPlan.getAllRanges().keySet()) { // foreach partition
			if(partitionTotals.get(i) > meanAccesses || i.intValue() >= partitionCount) { 
				List<List<Plan.Range>> partitionSlices = oldPlan.getRangeSlices(i,  coldPartitionWidth);
				if(partitionSlices.size() > 0) {
					Double tupleWeight = ((double) oldLoad.get(i)) / oldPlan.getTupleCount(i); // per tuple

					for(List<Plan.Range> slice : partitionSlices) {  // for each slice
						for(Plan.Range r : slice) { 
							Integer newWeight = (int) (tupleWeight *  ((double) Plan.getRangeWidth(r)));
							dstPartition = getMostUnderloadedPartitionId(partitionTotals, partitionCount);
							
							if((partitionTotals.get(i) > meanAccesses || i.intValue() >= partitionCount) && i != dstPartition) { 		
								
								List<Plan.Range> oldRanges = aPlan.getRangeValues(i, r.from, r.to);
								for(Plan.Range oldRange : oldRanges) {
									aPlan.removeRange(i, oldRange.from);
									if(!aPlan.hasPartition(dstPartition)) {
                                                       	 			aPlan.addPartition(dstPartition);
                                                			}
									aPlan.addRange(dstPartition, Math.max(oldRange.from, r.from), Math.min(oldRange.to, r.to));

									if(oldRange.from < r.from) {
										aPlan.addRange(i, oldRange.from, r.from - 1);
									}
									if(r.to < oldRange.to) {
										aPlan.addRange(i, r.to + 1, oldRange.to);
									}
								}
								
								partitionTotals.put(i, partitionTotals.get(i) - newWeight);
								partitionTotals.put(dstPartition, partitionTotals.get(dstPartition) + newWeight);
							}
						}
					} // end for each slice
				}
			} 
		} // end for each partition

		removeEmptyPartitions(aPlan);
		return aPlan;
		
	}
	

}
