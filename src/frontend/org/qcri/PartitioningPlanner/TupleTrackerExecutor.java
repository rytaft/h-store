package org.qcri.PartitioningPlanner;

import java.util.Collection;
import java.util.Map;

import org.voltdb.catalog.Site;

public class TupleTrackerExecutor {
	Collection<Site> allSites;
	Map<Site,Collection<Integer>> siteToPartitions;	
	Map<Integer, Site> partitionToSite;
	
	public TupleTrackerExecutor(){
		
	}
	
	// hotTuples: tupleId --> access count
	// siteLoads: partitionId --> total access count
	
	
}
