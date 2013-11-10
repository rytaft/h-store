package org.qcri.PartitioningPlanner;

//import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.io.*;

import org.voltdb.catalog.Site;

public class TupleTrackerExecutor {
	//Collection<Site> allSites;
	//Map<Site,Collection<Integer>> siteToPartitions;	
	//Map<Integer, Site> partitionToSite;
	
	// hotTuples: tupleId --> access count
	HashMap<Integer, Integer> hotTuples;
	// siteLoads: partitionId --> total access count
	HashMap<Integer, Integer> siteLoad;
	
	public TupleTrackerExecutor(){
		
	}
	
   public void getTopKPerPart(int k){
		
	}
	
	// hotTuples: tupleId --> access count
	// siteLoads: partitionId --> total access count
	
	
}
