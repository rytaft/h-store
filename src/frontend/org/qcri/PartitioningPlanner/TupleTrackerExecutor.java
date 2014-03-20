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
        
		//interval = 1; //turn on the tracking
        //client.callProcedure("@Statistics", statsType, interval);
        //System.out.println("Tuple Tracking has been turned on for "+seconds+" seconds");
        
		//turning on 
        String confNames[] = { "site.exec_readwrite_tracking"};
        String confValuesOn[] = {"true"};
        String confValuesOff[] = {"false"};
        client.callProcedure("@SetConfiguration", confNames, confValuesOn);
        		
		try
		  {
		  Thread.sleep(seconds*1000);  
		 
		  }catch (InterruptedException ie)
		  {
		  System.out.println(ie.getMessage());
		  }
		
		
		client.callProcedure("@Statistics", statsType, interval);
		
		client.callProcedure("@SetConfiguration", confNames, confValuesOff);
		
		System.out.println("Tuple Tracking collectted hot tuples and has been turned off");
		
	}



   

private int getNoOfTuplesPart(Map<Integer, Integer> PartNUM_VOTES, Integer pID) throws Exception
{
	int n = 0;
	//*
	Integer noVotes = PartNUM_VOTES.get(pID);
	if (noVotes != null) // does exist
	{ n = noVotes.intValue();
	//System.out.printf("Partition NumVotes is fected from the Map \n");
	}
	else 
	{
	
		System.out.printf("Partition " + pID.intValue() + "does not have a record in PartNUM_VOTES.\n");
	}
	return n;
}

private int getNoOfTuples(Map<Long, Integer> PhoneNUM_VOTES, Long phoneNo, org.voltdb.client.Client client) throws Exception
{
	int n ;
	//*
	Integer noVotes = PhoneNUM_VOTES.get(phoneNo);
	if (noVotes != null) // does exist
	{ n = noVotes.intValue();
	//System.out.printf("Phone NumVotes is fected from the Map \n");
	}
	else 
	//*/
	{
	
	
	String query = "select NUM_VOTES from V_VOTES_BY_PHONE_NUMBER where PHONE_NUMBER = " + phoneNo;
	//System.out.printf("Query:: " + query);
	ClientResponse cresponse = client.callProcedure("@AdHoc", query);
	VoltTable[] count = cresponse.getResults(); 
	
	if (count[0].getRowCount() == 0) {
                n = 0;
        }
        else {
                n = (int) count[0].fetchRow(0).getLong(0); // the NUM_VOTES of a specific phone no
	}	

	//System.out.printf("Phone no is " + phoneNo+ " has " + n + " tuples \n" );
	}
	return n;
}

private void fetchTuplesPerPart(Map<Integer, Integer> PartNUM_VOTES, org.voltdb.client.Client client) throws Exception {
	
    
	
	String statsType = "TABLE";
	int interval = 0;
	
	VoltTable[] results = client.callProcedure("@Statistics", statsType, interval).getResults();
	
	VoltTableRow row;
	int partition;
	int numOfPhones;
	
	int rowCount = results[0].getRowCount();

	//System.out.printf("results[0].getRowCount() = " +results[0].getRowCount()+"\n");
	
	for(int r = 0;r<rowCount;r++)
	{
		row = results[0].fetchRow(r);
		
		if (row.getString(5).equalsIgnoreCase("VOTES") )
		{
		partition = (int) row.getLong(4);
		numOfPhones   =  (int) row.getLong(8);
		PartNUM_VOTES.put(partition, numOfPhones);
		//System.out.printf(partition +", "+numOfPhones+"\n");
		}
				
	}
}	

private void fetchTuplesPerPhone(Map<Long, Integer> PhoneNUM_VOTES, org.voltdb.client.Client client) throws Exception
{
	String query;
	ClientResponse cresponse;
	
	
	query = "select count(*) from V_VOTES_BY_PHONE_NUMBER";
	//System.out.printf("Query:: " + query);
	cresponse = client.callProcedure("@AdHoc", query);
	VoltTable[] count = cresponse.getResults(); 
	
	//System.out.printf("Phone Count is " + count[0].fetchRow(0).getLong(0) +"\n");
	
	int i = (int) ((count[0].fetchRow(0).getLong(0)) / 100) ; // no phone numbers
	
	query = "select PHONE_NUMBER, NUM_VOTES from V_VOTES_BY_PHONE_NUMBER Order By NUM_VOTES DESC Limit " + i;
	//System.out.printf("Query:: " + query+"\n");
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
		//System.out.printf(phone +", "+num+"\n");
		
	}

	//System.out.printf("the top 1 percent of V_VOTES_BY_PHONE_NUMBER Order By NUM_VOTES is Fetched");
}



public void getTopKPerPart(int noPartitions, ArrayList<Map<Long, Pair<Long,Integer>>> htList, org.voltdb.client.Client client) throws Exception {
		
	Map<Long, Integer> PhoneNUM_VOTES;
	PhoneNUM_VOTES = new HashMap<Long,Integer>();
	fetchTuplesPerPhone(PhoneNUM_VOTES, client);
		
		
		
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
	            		                                        Integer.valueOf( getNoOfTuples(PhoneNUM_VOTES, Long.parseLong(parts[1]), client ))
	            		     ));
	        }
			reader.close();
			htList.add(hotTuples);
			
			//System.out.println("Essam hotTuplesList at partition"+i+" has " + hotTuples.size()); 
		    
		}
		
		
	   
		PhoneNUM_VOTES.clear();
		
	}
	
	
    public void getPartitionLoad(int noPartitions, Map<Integer, Pair<Long,Integer>> mPLoad, org.voltdb.client.Client client) throws Exception  {
    	
    	Map<Integer, Integer> PartNUM_VOTES;
    	PartNUM_VOTES = new HashMap<Integer, Integer>();
    	
    	
    	fetchTuplesPerPart(PartNUM_VOTES, client);
    	
    	BufferedReader reader;
		String fNPrefix ="./siteLoadPID_";
		String line;
		
		
		
		for (int i = 0; i < noPartitions; i++) {
			
			reader = new BufferedReader(new FileReader(fNPrefix+i+".del"));
			if ((line = reader.readLine()) != null) {
	            String parts[] = line.split("\t");
	            mPLoad.put(Integer.parseInt(parts[0]), Pair.of( Long.parseLong(parts[1]), getNoOfTuplesPart(PartNUM_VOTES, Integer.parseInt(parts[0]))	));
	            
	            System.out.println("Partition "+Integer.parseInt(parts[0])+" has <freq: "+Long.parseLong(parts[1])+", numTuple: "+ getNoOfTuplesPart(PartNUM_VOTES, Integer.parseInt(parts[0])) +">.\n");
	            
	        }
			reader.close();
			
			
		}
	   
		PartNUM_VOTES.clear();
		
	}
	
	
	
	
}
