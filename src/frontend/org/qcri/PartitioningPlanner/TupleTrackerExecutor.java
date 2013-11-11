package org.qcri.PartitioningPlanner;

//import java.util.Collection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.io.*;

import org.voltdb.catalog.Site;

public class TupleTrackerExecutor {
	//Collection<Site> allSites;
	//Map<Site,Collection<Integer>> siteToPartitions;	
	//Map<Integer, Site> partitionToSite;
	
	// hotTuples: tupleId --> access count
	// siteLoads: partitionId --> total access count
	
		
	public TupleTrackerExecutor(){
		
	}
	
   
	public void getTopKPerPart(int noPartitions, ArrayList<Map<Long, Long>> htList) throws Exception {
		
		Map<Long, Long> hotTuples;
		BufferedReader reader;
		String fNPrefix ="./hotTuplesPID_";
		String line;
		
		
		
		for (int i = 0; i < noPartitions; i++) {
			
			reader = new BufferedReader(new FileReader(fNPrefix+i+".del"));
			hotTuples = new HashMap<Long, Long>();
			line = reader.readLine(); // escape first line
			
			while ((line = reader.readLine()) != null) {
	            String parts[] = line.split("\t");
	            hotTuples.put(Long.parseLong(parts[1]), Long.parseLong(parts[2]));
	        }
			reader.close();
			htList.add(hotTuples);
		    
		}
		
		
	   
	   
		
	}
	
	
    public void getSiteLoadPerPart(int noPartitions, Map<Integer, Long> mSLoad) throws Exception  {
    	
    	BufferedReader reader;
		String fNPrefix ="./siteLoadPID_";
		String line;
		
		
		
		for (int i = 0; i < noPartitions; i++) {
			
			reader = new BufferedReader(new FileReader(fNPrefix+i+".del"));
			if ((line = reader.readLine()) != null) {
	            String parts[] = line.split("\t");
	            mSLoad.put(Integer.parseInt(parts[0]), Long.parseLong(parts[1]));
	        }
			reader.close();
			
		    
		}
	   
	   
		
	}
	
	
	
	
}
