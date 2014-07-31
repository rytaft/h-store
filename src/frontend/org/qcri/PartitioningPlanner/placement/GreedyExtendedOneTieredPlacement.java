package org.qcri.PartitioningPlanner.placement;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.utils.Pair;
import org.qcri.PartitioningPlanner.placement.Plan;
import org.voltdb.CatalogContext;


public class GreedyExtendedOneTieredPlacement extends Placement {
	
	Long coldPartitionWidth = 100000L; // redistribute cold tuples in chunks of 100000
	
	public GreedyExtendedOneTieredPlacement(){
		
	}
	
	
	// hotTuples: tupleId --> access count
	// siteLoads: partitionId --> total access count
	public Plan computePlan(ArrayList<Map<Long, Pair<Long,Integer> >> hotTuplesList, Map<Integer, Pair<Long,Integer>> partitionTotals, String planFilename, int partitionCount, int timeLimit, CatalogContext catalogContext){
		
		// ignore the hotTuplesList!
		
		Integer dstPartition = -1;
		Long totalAccesses = 0L;
		Long meanAccesses;
		
		Plan aPlan = new Plan(planFilename);

		for(int i = 0; i < partitionCount; ++i) {
		    if(partitionTotals.get(i) == null) {
		    	partitionTotals.put(i, new Pair<Long, Integer>(0L, 0));
		    }
		}
		
		// copy partitionTotals into oldLoad
		Map<Integer, Pair<Long,Integer>> oldLoad = new HashMap<Integer, Pair<Long,Integer>> ();
		for(Integer i : partitionTotals.keySet()) {
			totalAccesses += partitionTotals.get(i).getFirst();
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
			if(partitionTotals.get(i).getFirst() > meanAccesses || i.intValue() >= partitionCount) { 
				
				// VOTER HACK: we want each partition slice to contain ~1000 tuples, but we don't know how many tuples
				// are in a range
				Double tuplesPerKey = (double) oldLoad.get(i).getSecond() / Plan.getRangeListWidth(oldPlan.getAllRanges(i));
				List<List<Plan.Range>> partitionSlices = oldPlan.getRangeSlices(i,  (long) (coldPartitionWidth / tuplesPerKey));
				if(partitionSlices.size() > 0) {
					
					Double tupleWeight = (double) oldLoad.get(i).getFirst() / oldLoad.get(i).getSecond(); // per tuple - VOTER HACK
					
					for(List<Plan.Range> slice : partitionSlices) {  // for each slice
						for(Plan.Range r : slice) { 
							// VOTER HACK
							Integer sliceSize = (int) (Plan.getRangeListWidth(slice) * tuplesPerKey);
							Long newWeight = (long) (tupleWeight *  ((double) sliceSize));
							if(newWeight == 0 && i.intValue() < partitionCount) {
								continue;
							}

							dstPartition = getMostUnderloadedPartitionId(partitionTotals, partitionCount);
							
							if((partitionTotals.get(i).getFirst() > meanAccesses || i.intValue() >= partitionCount) && i != dstPartition) { 		
								
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
								
								partitionTotals.put(i, new Pair<Long, Integer>(partitionTotals.get(i).getFirst() - newWeight, 
										partitionTotals.get(i).getSecond() - sliceSize));
								partitionTotals.put(dstPartition, new Pair<Long, Integer>(partitionTotals.get(dstPartition).getFirst() + newWeight, 
										partitionTotals.get(dstPartition).getSecond() + sliceSize));
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
