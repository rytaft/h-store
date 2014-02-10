package org.qcri.PartitioningPlanner.placement;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.qcri.PartitioningPlanner.placement.Plan.Range;
import org.voltdb.catalog.Site;
import org.voltdb.utils.Pair;

public class Placement {
	Collection<Site> allSites;
	Map<Site,Collection<Integer>> siteToPartitions;	
	Map<Integer, Site> partitionToSite;
	
	public Placement(){
		
	}
	
	// hotTuples: tupleId --> access count
	// siteLoads: partitionId --> total access count
	// partitionCount: number of partitions actually in use
	// timeLimit - time limit for planner in ms
	public Plan computePlan(ArrayList<Map<Long, Pair<Long,Integer> >> hotTuplesList, Map<Integer, Pair<Long,Integer>> siteLoads, String planFile, int partitionCount, int timeLimit){
		return new Plan(planFile);
	}
	
	static Integer getMostUnderloadedPartitionId(Map<Integer, Pair<Long,Integer>> partitionTotals, int partitionCount) {
		Long minTotal = java.lang.Long.MAX_VALUE; 
		Integer minPartition = -1;

		for(Integer i : partitionTotals.keySet()) {
			if(i < partitionCount && partitionTotals.get(i).getFirst() < minTotal) {
				minPartition = i;
				minTotal = partitionTotals.get(i).getFirst();
			}
			
		}
		
		return minPartition;
	}
	
	static void getHottestTuplePartition(Map<Long, Pair<Long,Integer> > hotTuples, Long tupleId, Long accessCount) {
		accessCount = 0L;
		tupleId  = -1L;
		
		System.out.println("Iterating over " + hotTuples.size() + " tuples.");
	}
	
	
	void getHottestTuple(ArrayList<Map<Long, Pair<Long,Integer> >> hotTuplesList) {

		_hotAccessCount = 0L;
		_hotTupleId = -1L;
		_srcPartition = -1;
		_hotSize = 0;
		
		
		for(Integer i = 0; i < hotTuplesList.size(); ++i) {
			Long partitionTupleId = 0L;
			Long partitionAccessCount = 0L;
			Integer partitionSize = 0;

			for(Long j : hotTuplesList.get(i).keySet()) {
				if(hotTuplesList.get(i).get(j).getFirst() > partitionAccessCount) {
					partitionAccessCount = hotTuplesList.get(i).get(j).getFirst();
					partitionSize = hotTuplesList.get(i).get(j).getSecond();
					partitionTupleId = j;
				}
			}

			if(partitionAccessCount > _hotAccessCount) {
				_hotAccessCount = partitionAccessCount;
				_hotTupleId = partitionTupleId;
				_srcPartition = i;
				_hotSize = partitionSize;
			}
		}
	}
	
	Integer _srcPartition; 
	Long _hotTupleId; 
	Long _hotAccessCount;
	Integer _hotSize;
		
	// If tuples are no longer hot, put them back in their enclosing range
	static public Plan demoteTuples(ArrayList<Map<Long, Pair<Long,Integer> >> hotTuplesList, Plan plan) {
		// Get a lookup table of all currently hot tuples
		Set<Long> hotTuplesLookup = new HashSet<Long>();
		for(Map<Long, Pair<Long,Integer> >  hotTuples : hotTuplesList) {
			hotTuplesLookup.addAll(hotTuples.keySet());
		}
		
		// go through the current plan and find old hot tuples by identifying
		// ranges of size 1
		for(Integer partitionId : plan.getAllPartitions()) {
			for(Plan.Range range : plan.getAllRanges(partitionId)) {
				// test if the old hot tuple is no longer hot
				if(Plan.getRangeWidth(range) == 1 && !hotTuplesLookup.contains(range.from)) {
					Long tupleId = range.from;
					// Move it to the partition containing the enclosing range
					Integer nextPartition = plan.getTuplePartition(tupleId + 1);
					if(nextPartition != null) {
						plan.removeTupleId(partitionId, tupleId);
						plan.addRange(nextPartition, tupleId, tupleId);
					}
					else {
						Integer prevPartition = plan.getTuplePartition(tupleId - 1);
						if(prevPartition != null) {
							plan.removeTupleId(partitionId, tupleId);
							plan.addRange(prevPartition, tupleId, tupleId);
						}
					}
				}
			}
		}
		return plan;
	}
	
	static public void removeEmptyPartitions(Plan aPlan) {
	    Set<Integer> partitions = new HashSet<Integer>();
	    partitions.addAll(aPlan.getAllPartitions());
	    for(Integer partitionId : partitions) {
		if(aPlan.getAllRanges(partitionId).isEmpty()) {
		    aPlan.removePartition(partitionId);
		}
	    }
	}

}
