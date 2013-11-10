package org.qcri.PartitioningPlanner.placement;


import java.util.ArrayList;
import java.util.Map;

import org.qcri.PartitioningPlanner.placement.Plan;


public class GreedyPlacement extends Placement {
	
	public GreedyPlacement(){
		
	}
	
	static Integer getMostUnderloadedPartitionId(Map<Integer, Integer> partitionTotals) {
		Integer minTotal = java.lang.Integer.MAX_VALUE; 
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
	public Plan computePlan(ArrayList<Map<Integer, Integer>> hotTuplesList, Map<Integer, Integer> partitionTotals, Plan aPlan){
		
		Map<Integer, Integer> hotTuples;
		
		int no_of_partitions = hotTuplesList.size();
		
		hotTuples = hotTuplesList.get(0); //hot tuples at partition 0;
			
		
		Integer srcPartition, dstPartition;
		Integer totalAccesses = 0;
		Integer meanAccesses;
		

		for(Integer i : partitionTotals.keySet()) {
			totalAccesses = totalAccesses + partitionTotals.get(i);			
		}
		
		meanAccesses = totalAccesses / partitionTotals.size();

		System.out.println("Mean access count: " + meanAccesses);
		
		for(Integer i : hotTuples.keySet()) {
			srcPartition = aPlan.getTuplePartition(i);
			if(partitionTotals.get(srcPartition) > meanAccesses) {
				dstPartition = getMostUnderloadedPartitionId(partitionTotals);

				partitionTotals.put(srcPartition, partitionTotals.get(srcPartition)  - hotTuples.get(i));
				partitionTotals.put(dstPartition,partitionTotals.get(dstPartition)  + hotTuples.get(i));
				aPlan.removeTupleId(srcPartition, i);
				aPlan.addRange(dstPartition, i, i);
			}
			
		}

		return aPlan;
		
	}
	

}
