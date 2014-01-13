package org.qcri.PartitioningPlanner.placement;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltdb.catalog.Site;

public class Placement {
	Collection<Site> allSites;
	Map<Site,Collection<Integer>> siteToPartitions;	
	Map<Integer, Site> partitionToSite;
	
	public Placement(){
		
	}
	
	// hotTuples: tupleId --> access count
	// siteLoads: partitionId --> total access count
	// partitionCount: number of partitions actually in use
	public Plan computePlan(ArrayList<Map<Long, Long>> hotTuplesList, Map<Integer, Long> siteLoads, String planFile, int partitionCount){
		return new Plan(planFile);
	}
	
	static Integer getMostUnderloadedPartitionId(Map<Integer, Long> partitionTotals, int partitionCount) {
		Long minTotal = java.lang.Long.MAX_VALUE; 
		Integer minPartition = -1;

		for(Integer i : partitionTotals.keySet()) {
			if(i < partitionCount && partitionTotals.get(i) < minTotal) {
				minPartition = i;
				minTotal = partitionTotals.get(i);
			}
			
		}
		
		return minPartition;
	}
	
	static void getHottestTuplePartition(Map<Long, Long> hotTuples, Long tupleId, Long accessCount) {
		accessCount = 0L;
		tupleId  = -1L;
		
		System.out.println("Iterating over " + hotTuples.size() + " tuples.");
	}
	
	
	void getHottestTuple(ArrayList<Map<Long, Long>> hotTuplesList) {

		_hotAccessCount = 0L;
		_hotTupleId = -1L;
		_srcPartition = -1;
		
		
		for(Integer i = 0; i < hotTuplesList.size(); ++i) {
			Long partitionTupleId = 0L;
			Long partitionAccessCount = 0L;

			for(Long j : hotTuplesList.get(i).keySet()) {
				if(hotTuplesList.get(i).get(j) > partitionAccessCount) {
					partitionAccessCount = hotTuplesList.get(i).get(j);
					partitionTupleId = j;
				}
			}

			if(partitionAccessCount > _hotAccessCount) {
				_hotAccessCount = partitionAccessCount;
				_hotTupleId = partitionTupleId;
				_srcPartition = i;

			}
		}
	}
	
	Integer _srcPartition; 
	Long _hotTupleId; 
	Long _hotAccessCount;
		
	// If tuples are no longer hot, put them back in their enclosing range
	public Plan demoteTuples(ArrayList<Map<Long, Long>> hotTuplesList, Plan plan) {
		// Get a lookup table of all currently hot tuples
		Set<Long> hotTuplesLookup = new HashSet<Long>();
		for(Map<Long, Long>  hotTuples : hotTuplesList) {
			hotTuplesLookup.addAll(hotTuples.keySet());
		}
		
		// go through the current plan and find old hot tuples by identifying
		// ranges of size 1
		ArrayList<List<Plan.Range>> rangesList = new ArrayList<List<Plan.Range>>();
		rangesList.addAll(plan.getAllRanges().values());
		int partitionId = 0;
		for(List<Plan.Range> ranges : rangesList) {
			for(Plan.Range range : ranges) {
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
			partitionId++;
		}
		return plan;
	}

}
