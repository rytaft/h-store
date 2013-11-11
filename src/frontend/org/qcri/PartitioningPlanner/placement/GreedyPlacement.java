package org.qcri.PartitioningPlanner.placement;


import java.util.ArrayList;
import java.util.Map;

import org.qcri.PartitioningPlanner.placement.Plan;


public class GreedyPlacement extends Placement {
	
	public GreedyPlacement(){
		
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
		
		
		
			
		
		Integer srcPartition, dstPartition;
		Long totalAccesses = 0L;
		Long meanAccesses;
		Integer partitionId = 0;
		

		for(Integer i : partitionTotals.keySet()) {
			totalAccesses = totalAccesses + partitionTotals.get(i);			
		}
		
		meanAccesses = totalAccesses / partitionTotals.size();

		System.out.println("Mean access count: " + meanAccesses);
		
		for(Map<Long, Long>  hotTuples : hotTuplesList) {
			srcPartition = partitionId;
			for(Long i : hotTuples.keySet()) {
				if(partitionTotals.get(srcPartition) > meanAccesses) {
					dstPartition = getMostUnderloadedPartitionId(partitionTotals);

					partitionTotals.put(srcPartition, partitionTotals.get(srcPartition)  - hotTuples.get(i));
					partitionTotals.put(dstPartition,partitionTotals.get(dstPartition)  + hotTuples.get(i));
					aPlan.removeTupleId(srcPartition, i);
					aPlan.addRange(dstPartition, i, i);
				}
			}
			++partitionId;
		}

		return aPlan;
		
	}
	

}
