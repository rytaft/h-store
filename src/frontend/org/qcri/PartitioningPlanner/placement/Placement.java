package org.qcri.PartitioningPlanner.placement;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.voltdb.catalog.Site;

public class Placement {
	Collection<Site> allSites;
	Map<Site,Collection<Integer>> siteToPartitions;	
	Map<Integer, Site> partitionToSite;
	
	public Placement(){
		
	}
	
	// hotTuples: tupleId --> access count
	// siteLoads: partitionId --> total access count
	public Plan computePlan(ArrayList<Map<Long, Long>> hotTuplesList, Map<Integer, Long> siteLoads, String planFile){
		return new Plan(planFile);
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

}
