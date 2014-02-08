package org.qcri.PartitioningPlanner;

//import java.util.Collection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.io.*;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
//import org.voltdb.VoltTableRow;
//import org.voltdb.client.ClientConfig;
//import org.voltdb.client.Client;
//import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.utils.Pair;

public class TupleTrackerExecutor {
	//Collection<Site> allSites;
	//Map<Site,Collection<Integer>> siteToPartitions;	
	//Map<Integer, Site> partitionToSite;
	
	// hotTuples: tupleId --> access count
	// siteLoads: partitionId --> total access count
 	
	private Map<Long, Integer> PhoneNUM_VOTES; 	
	public TupleTrackerExecutor(){
		
	}
	
	
	public void runTestCase() throws Exception {
		
		int port = 21212;
		String host = "localhost";
		String statsType = "TUPLE";
		int interval = 0;
		
		//ClientConfig clientConfig = new ClientConfig("program", "none");
        org.voltdb.client.Client client =
            org.voltdb.client.ClientFactory.createClient();
        
        // Client instance connected to the database running on
        // the specified IP address, in this case 127.0.0.1. The
        // database always runs on TCP/IP port 21212.
        client.createConnection(host,port);
		
		String query = "SELECT COUNT(*) FROM WAREHOUSE WHERE W_ID = 1";
		ClientResponse cresponse = client.callProcedure("@AdHoc", query);
		VoltTable[] count = cresponse.getResults(); 
		System.out.printf("Found WAREHOUSE no %d.\n", count[0].fetchRow(0).getLong(0));
		
		ClientResponse results = client.callProcedure("@Statistics", statsType, interval);
		
		
		
	}
	
	
	
public void turnOnOff(int seconds, org.voltdb.client.Client client) throws Exception {
		
		String statsType = "TUPLE";
		int interval = 0;
		
		//ClientConfig clientConfig = new ClientConfig("program", "none");
        //org.voltdb.client.Client client =   org.voltdb.client.ClientFactory.createClient();
        
        // Client instance connected to the database running on
        // the specified IP address, in this case 127.0.0.1. The
        // database always runs on TCP/IP port 21212.
        //client.createConnection(host,port);
        
        //System.out.println("Tuple Tracking: Connected host");
		
		//ClientResponse results = client.callProcedure("@Statistics", statsType, interval);
        
        client.callProcedure("@Statistics", statsType, interval);
        System.out.println("Tuple Tracking has been turned on for "+seconds+" seconds");
		
		try
		  {
		  Thread.sleep(seconds*1000);  
		 
		  }catch (InterruptedException ie)
		  {
		  System.out.println(ie.getMessage());
		  }
		
		client.callProcedure("@Statistics", statsType, interval);
		System.out.println("Tuple Tracking collectted hot tuples and has been turned off");
		
	}
   

private int getNoOfTuples(Long phoneNo, org.voltdb.client.Client client) throws Exception
{
	int n ;
	Integer noVotes = PhoneNUM_VOTES.get(phoneNo);
	if (noVotes != null) // does exist
	  n = noVotes.intValue();
	else {
	
	
	String query = "select NUM_VOTES from V_VOTES_BY_PHONE_NUMBER where PHONE_NUMBER = " + phoneNo;
	//System.out.printf("Query:: " + query);
	ClientResponse cresponse = client.callProcedure("@AdHoc", query);
	VoltTable[] count = cresponse.getResults(); 
	
	n = (int) count[0].fetchRow(0).getLong(0); // the NUM_VOTES of a specific phone no
	
	//System.out.printf("Phone no is " + phoneNo+ " has " + n + " tuples \n" );
	}
	return n;
}

public void fetchNoOfTuples(org.voltdb.client.Client client) throws Exception
{
	String query;
	ClientResponse cresponse;
	PhoneNUM_VOTES = new HashMap<Long,Integer>();
	
	query = "select count(*) from V_VOTES_BY_PHONE_NUMBER";
	//System.out.printf("Query:: " + query);
	cresponse = client.callProcedure("@AdHoc", query);
	VoltTable[] count = cresponse.getResults(); 
	
	int i = (int) (count[0].fetchRow(0).getLong(0))/100 ; // no phone numbers
	
	query = "select PHONE_NUMBER, NUM_VOTES from V_VOTES_BY_PHONE_NUMBER Order By NUM_VOTES DESC Limit " + i;
	System.out.printf("Query:: " + query);
	cresponse = client.callProcedure("@AdHoc", query);
	VoltTable[] reslt = cresponse.getResults(); 
	
	VoltTableRow row;
	long phone;
	int num;
	int r = 0;
	for (r = 0 ; r< i; r++)
	{
		row = reslt[0].fetchRow(r);
		//System.out.printf("Got Row");
		phone =  row.getLong(0);
		//System.out.printf("Got Phone " + phone);
		num   =  (int) row.getLong(1);
		//System.out.printf("Got Votes " + num);
		PhoneNUM_VOTES.put(phone,num);
		System.out.printf(phone +", "+num+"\n");
		
	}

	System.out.printf("the Top 1% of V_VOTES_BY_PHONE_NUMBER Order By NUM_VOTES is Fetched");
}

public void eraseNoOfTuples(){PhoneNUM_VOTES.clear();}

	public void getTopKPerPart(int noPartitions, ArrayList<Map<Long, Pair<Long,Integer>>> htList, org.voltdb.client.Client client) throws Exception {
		
		
		
		
		
		
		//Map<Long, Long> hotTuples;
				
		Map<Long, Pair<Long,Integer>> hotTuples;
		
		BufferedReader reader;
		String fNPrefix ="./hotTuplesPID_";
		String line;
		
		
		
		for (int i = 0; i < noPartitions; i++) {
			
			reader = new BufferedReader(new FileReader(fNPrefix+i+".del"));
			hotTuples = new HashMap<Long, Pair<Long,Integer>>();
			line = reader.readLine(); // escape first line
			
			while ((line = reader.readLine()) != null) {
	            String parts[] = line.split("\t");
	            //hotTuples.put(Long.parseLong(parts[1]), Long.parseLong(parts[2])); 
	           
	            hotTuples.put(Long.parseLong(parts[1]), Pair.of(Long.parseLong(parts[2]), 
	            		                                        Integer.valueOf( getNoOfTuples( Long.parseLong(parts[1]), client ))
	            		     ));
	        }
			reader.close();
			htList.add(hotTuples);
			
			//System.out.println("Essam hotTuplesList at partition"+i+" has " + hotTuples.size()); 
		    
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
