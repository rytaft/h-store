package org.qcri.ehstore.placement;

import java.util.Collection;
import java.util.Map;

import org.voltdb.catalog.Site;

public class Placement {
	Collection<Site> allSites;
	Map<Site,Collection<Integer>> siteToPartitions;	
	Map<Integer, Site> partitionToSite;
	
	public Placement(){
		
	}
	
	public Plan computePlan(Collection<Site> hotSites, Plan currentPlan){
		return currentPlan;
	}
}
