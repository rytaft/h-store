package org.qcri.PartitioningPlanner;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.HashMap;


import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.FileSystems;
import java.nio.file.CopyOption;
import java.nio.file.StandardCopyOption;

import org.qcri.PartitioningPlanner.placement.Placement;
import org.qcri.PartitioningPlanner.placement.GreedyPlacement;
import org.qcri.PartitioningPlanner.placement.GreedyExtendedPlacement;
import org.qcri.PartitioningPlanner.placement.BinPackerPlacement;
import org.qcri.PartitioningPlanner.placement.FirstFitPlacement;
import org.qcri.PartitioningPlanner.placement.OneTieredPlacement;
import org.qcri.PartitioningPlanner.placement.Plan;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Site;
import org.voltdb.processtools.ShellTools;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;

public class Controller implements Runnable {
	
	private org.voltdb.client.Client client;
	private String connectedHost;
	private Collection<Site> sites;
	
	private Placement algo;
	private Plan currentPlan;
	private Path planFile;
	private Path outputPlanFile;
	
	
	private TupleTrackerExecutor ttExecutor;
	
	
	private static final double CPU_THRESHOLD = 0.8;
	private static final int PARTITIONS_PER_HOST = 8;
	private static final int POLL_FREQUENCY = 3000;
	
	private static int time_window = 10; // time window for tuple tracking
	
	private static int planner_selector = 0; // planner ID from 0 to 4, Greedy - GreedyEx - FFit - BP - BP one tier 
	
	private static int no_of_partitions = 4; 
	
	
	

	// used HStoreTerminal as model to handle the catalog
	public Controller (Catalog catalog){
		
		if(connectedHost == null){
		    connectToHost();
		}
		
		
		
		switch (planner_selector) {
        case 0:  algo = new GreedyPlacement(); System.out.println("GreedyPlacement is selected"); break;
        case 1:  algo = new GreedyExtendedPlacement(); System.out.println("GreedyExtendedPlacement is selected"); break;
        case 2:  algo = new FirstFitPlacement(); System.out.println("FirstFitPlacement is selected"); break;
        case 3:  algo = new BinPackerPlacement(); System.out.println("BinPackerPlacement is selected"); break;
        case 4:  algo = new OneTieredPlacement(); System.out.println("OneTieredPlacement is selected"); break;
       	}

		//algo = new Placement();	
	   // algo = new GreedyPlacement();
	   // algo = new GreedyExtendedPlacement();
	   // algo = new BinPackerPlacement();
	    //algo = new FirstFitPlacement();
	    //algo = new OneTieredPlacement();		

		ttExecutor = new TupleTrackerExecutor();
		// connect to VoltDB server
        client = ClientFactory.createClient();
        client.configureBlocking(false);
        sites = CatalogUtil.getAllSites(catalog);
        
        HStoreConf hStoreConf = HStoreConf.singleton();
        if(hStoreConf.get("global.hasher_plan") == null){
        	System.out.println("Must set global.hasher_plan to specify plan file!");
        	System.out.println("Using default (plan.json)");
        	planFile = FileSystems.getDefault().getPath("plan.json");
            
        }
        else{
        	planFile = FileSystems.getDefault().getPath(hStoreConf.get("global.hasher_plan").toString());
        }
        
        outputPlanFile = FileSystems.getDefault().getPath("plan_out.json");

	try {
	    Files.copy(planFile, outputPlanFile, StandardCopyOption.REPLACE_EXISTING);				
	}
	catch(IOException e) {
	    System.out.println("Controller: IO Exception while copying plan file to output plan file");
	    e.printStackTrace();
	}

	}

	@Override
	public void run () {
	   				
					//Jennie temp for now
					Map<Integer, Long> mSiteLoad = new HashMap<Integer, Long>();
					
					ArrayList<Map<Long, Long>> hotTuplesList = new ArrayList<Map<Long, Long>> (no_of_partitions);
					
					
					
					
					try {
						
					//ttExecutor.runTestCase(); 	
					//System.out.println("Essam Before: hotTuplesList size is " + hotTuplesList.size());
					
					
						
				    ttExecutor.turnOnOff(time_window,client);	// turn on tuple tracking for time window of X seconds
						
					// here we get top K
					ttExecutor.getTopKPerPart(no_of_partitions,hotTuplesList);
					
					// here we get load per site
					ttExecutor.getSiteLoadPerPart(no_of_partitions,mSiteLoad);
					
					//System.out.println("Essam After: hotTuplesList size is " + hotTuplesList.size());
					
					// here we call the planner
					// @todo - last parameter should be the number of partitions in use - may be less than

					// hotTuplesList.size()					
					currentPlan = algo.computePlan(hotTuplesList, mSiteLoad, planFile.toString(), hotTuplesList.size());
					//currentPlan = algo.computePlan(hotTuplesList, mSiteLoad, planFile.toString(), 3);
					currentPlan.toJSON(outputPlanFile.toString());


						
 						ClientResponse cresponse = null;
						try {
						    cresponse = client.callProcedure("@Reconfiguration", 0, outputPlanFile.toString(), "livepull");
						    System.out.println("Controller: received response: " + cresponse);
						} catch (NoConnectionsException e) {
						    System.out.println("Controller: lost connection");
						    e.printStackTrace();
						} catch (IOException e) {
						    System.out.println("Controller: IO Exception while connecting to host");
						    e.printStackTrace();
						} catch (ProcCallException e) {
						    System.out.println("Controller: @Reconfiguration transaction rejected (backpressure?)");
						    e.printStackTrace();
						}

						if(cresponse.getStatus() != Status.OK){
						    System.out.println("@Reconfiguration transaction aborted");
						}

					} catch(Exception e) {
						System.out.println("Caught on exception " + e.toString());
					}
				
				
	}
	
	private void connectToHost(){
        Site catalog_site = CollectionUtil.random(sites);
        connectedHost= catalog_site.getHost().getIpaddr();

        try {
			client.createConnection(null, connectedHost, HStoreConstants.DEFAULT_PORT, "user", "password");
		} catch (UnknownHostException e) {
			System.out.println("Controller: tried to connect to unknown host");
			e.printStackTrace();
			return;
		} catch (IOException e) {
			System.out.println("Controller: IO Exception while connecting to host");
			e.printStackTrace();
			return;
		}
        System.out.println("Connected to host " + connectedHost);
	}

	private boolean checkLatencies(){
		if(connectedHost == null){
			connectToHost();
		}
        String statsType = "TXNRESPONSETIME";
        int interval = 0;
        ClientResponse cresponse = null;
		try {
			cresponse = client.callProcedure("@Statistics", statsType, interval);
		} catch (NoConnectionsException e) {
			System.out.println("Controller: lost connection");
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			System.out.println("Controller: IO Exception while connecting to host");
			e.printStackTrace();
			return false;
		} catch (ProcCallException e) {
			System.out.println("Controller: @Statistics transaction rejected (backpressure?)");
			e.printStackTrace();
			return false;
		}
 
		if(cresponse.getStatus() != Status.OK){
			System.out.println("@Statistics transaction aborted");
			return false;
		}
		
    	System.out.println("Received stats, parsing...");
    	VoltTable stats = cresponse.getResults()[0];
    	
		String prevHost = stats.fetchRow(0).getString(2);
		long goodTxns = 0;
		long badTxns = 0;
		for(int r = 0; r < stats.getRowCount(); ++r){
			VoltTableRow row = stats.fetchRow(r);
			String currHost = row.getString(2);
			if (!prevHost.equals(currHost)){
				// finished the rows related to the previous host
				if (badTxns != 0){
					if (goodTxns == 0 || badTxns/goodTxns > 0.05) return false;
				}
				System.out.println("Host " + prevHost +" - good transactions: " + goodTxns + "; bad transactions: " + badTxns);
				goodTxns = 0;
				badTxns = 0;
				prevHost = currHost; 
			}
			
			goodTxns += row.getLong(4) + row.getLong(5) + row.getLong(6);
			badTxns += row.getLong(7);
//			System.out.println("Row " + r);
//			System.out.println("\tPartition: " + row.getLong(1));
//			System.out.println("\tHost: " + row.getString(2));
//			System.out.println("\tProcedure: " + row.getString(3));
//			System.out.println("\tCount-100: " + row.getLong(4));
		}
		if (badTxns != 0){
			if (goodTxns == 0 || badTxns/goodTxns > 0.05) return false;
		}
		System.out.println("Host " + prevHost +" - good transactions: " + goodTxns + "; bad transactions: " + badTxns);
		return true;
	}
	
	public List<Site> getOverloadedSites() throws InterruptedException{
		ArrayList<Site> res = new ArrayList<Site> (sites.size());
		for(Site s : sites){
			String ip = s.getHost().getIpaddr();
			double cpuUsage = getCPUUtil(ip);
	        if (cpuUsage > CPU_THRESHOLD * PARTITIONS_PER_HOST){
	        	res.add(s);
	        }
		}
		return res;
	}
	
	public static double getCPUUtil(String ip){
        System.out.println("Polling " + ip);
        String results = ShellTools.cmd("ssh " + ip + " ps -eo pcpu");
//        String results = ShellTools.cmd("ssh " + ip + " ls -l");
        String[] lines = results.split("\n");
        if (lines.length < 2){
        	System.out.println("Controller: Problem while polling CPU usage for host " + ip);
        	System.exit(-1);
        }
        double cpuUsage = 0;
        for (int i = 1; i < lines.length; ++i){
        	cpuUsage += Double.parseDouble(lines[i]);
        }
        System.out.println("CPU usage of host " + ip + ": " + cpuUsage);
        return cpuUsage;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] vargs) throws Exception{
		
		if(vargs.length == 0){
			System.out.println("Must specify server hostname");
			return;
		}		
        
		String[] params = {vargs[0]};
		
		ArgumentsParser args = ArgumentsParser.load(params,ArgumentsParser.PARAM_CATALOG);
		
		//System.out.println("Params: bench"+params[0] +" no. part " +vargs[1] + " twin "+vargs[2]+" plannerID "+vargs[3]);
		
		if(vargs.length == 4)
		{
		no_of_partitions = Integer.parseInt(vargs[1]);
		time_window = Integer.parseInt(vargs[2]);
		planner_selector = Integer.parseInt(vargs[3]);
		}
		else // use default
		{
			no_of_partitions = 4;
			time_window = 10;
			planner_selector = 0;
		}

        

        Controller c = new Controller(args.catalog);
       	c.run();
	}
}
