package org.qcri.PartitioningPlanner.placement;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.utils.Pair;
import org.qcri.PartitioningPlanner.placement.Plan;


public class GreedyExtendedPlacement extends Placement {
	
	Long coldPartitionWidth = 100000L; // redistribute cold tuples in chunks of 100000
	
	public GreedyExtendedPlacement(){
		
	}
	
	
	// hotTuples: tupleId --> access count
	// siteLoads: partitionId --> total access count
	public Plan computePlan(ArrayList<Map<Long, Pair<Long,Integer> >> hotTuplesList, Map<Integer, Pair<Long,Integer>> partitionTotals, String planFilename, int partitionCount, int timeLimit){
		
		Integer dstPartition = -1;
		Long totalAccesses = 0L;
		Long meanAccesses;
		Long hotTupleCount = 0L;
		
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
		
		// calculate the load and plan if the hot tuples were removed, store
		// them in oldLoad and oldPlan
		Integer partitionId = 0;
		for(Map<Long, Pair<Long,Integer> >  hotTuples : hotTuplesList) {
			for(Long i : hotTuples.keySet()) {
				oldLoad.put(partitionId, new Pair<Long, Integer>(oldLoad.get(partitionId).getFirst() - hotTuples.get(i).getFirst(), 
						oldLoad.get(partitionId).getSecond() - hotTuples.get(i).getSecond()));
				oldPlan.removeTupleId(partitionId, i);
			}
			++partitionId;
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
		
		aPlan = demoteTuples(hotTuplesListCopy, aPlan);
		removeEmptyPartitions(aPlan);
		return aPlan;
		
	}
	

}
