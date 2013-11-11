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
	
   
	public void getTopKPerPart(int noPartitions, ArrayList<Map<Integer, Integer>> htList) throws Exception {
		
		Map<Integer, Integer> hotTuples;
		BufferedReader reader;
		String fNPrefix ="./hotTuplesPID_";
		String line;
		
		
		
		for (int i = 0; i < noPartitions; i++) {
			
			reader = new BufferedReader(new FileReader(fNPrefix+i+".del"));
			hotTuples = new HashMap<Integer, Integer>();
			line = reader.readLine(); // escape first line
			
			while ((line = reader.readLine()) != null) {
	            String parts[] = line.split("\t");
	            hotTuples.put(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
	        }
			reader.close();
			htList.add(hotTuples);
		    
		}
		
		
	   
	   
		
	}
	
	
    public void getSiteLoadPerPart(int noPartitions, Map<Integer, Integer> mSLoad) throws Exception  {
    	
    	BufferedReader reader;
		String fNPrefix ="./siteLoadPID_";
		String line;
		
		
		
		for (int i = 0; i < noPartitions; i++) {
			
			reader = new BufferedReader(new FileReader(fNPrefix+i+".del"));
			if ((line = reader.readLine()) != null) {
	            String parts[] = line.split("\t");
	            mSLoad.put(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
	        }
			reader.close();
			
		    
		}
	   
	   
		
	}
	
	
	
	
}
