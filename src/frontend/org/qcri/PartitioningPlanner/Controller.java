package org.qcri.PartitioningPlanner;

import java.io.IOException;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.HashMap;


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
	
	private Client client;
	private String connectedHost;
	private Collection<Site> sites;
	
	private Placement algo;
	private Plan currentPlan;
	private String planFile;
	
	
	private TupleTrackerExecutor ttExecutor;
	private Catalog catalog;
	
	
	private static int no_of_partitions = 4;

	// used HStoreTerminal as model to handle the catalog
	public Controller (Catalog catalog){
		//algo = new Placement();
		
		//Jennie: here we instaniate the planner algo
	    //algo = new GreedyPlacement();
	    algo = new GreedyExtendedPlacement();
	    //algo = new BinPackerPlacement();
	    //algo = new FirstFitPlacement();
	    //algo = new OneTieredPlacement();		

		ttExecutor = new TupleTrackerExecutor();
		// connect to VoltDB server
        client = ClientFactory.createClient();
        client.configureBlocking(false);
        sites = CatalogUtil.getAllSites(catalog);
        
        this.catalog = catalog;
        
        HStoreConf hStoreConf = HStoreConf.singleton();
        if(hStoreConf.get("global.hasher_plan") == null){
        	System.out.println("Must set global.hasher_plan to specify plan file!");
        	System.out.println("Going on (for testing)");
	        currentPlan = new Plan();
        }
        else{
	        planFile = hStoreConf.get("global.hasher_plan").toString();
	        currentPlan = new Plan(planFile);
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
					
					
						
					ttExecutor.turnOnOff(10);	// turn on tuple tracking for time window of X seconds
						
					// here we get top K
					ttExecutor.getTopKPerPart(no_of_partitions,hotTuplesList);
					
					// here we get load per site
					ttExecutor.getSiteLoadPerPart(no_of_partitions,mSiteLoad);
					
					//System.out.println("Essam After: hotTuplesList size is " + hotTuplesList.size());
					
					// here we call the planner
					// @todo - last parameter should be the number of partitions in use - may be less than
					// hotTuplesList.size()
					Integer currNoPartitions = 0;
					for (Integer part : mSiteLoad.keySet()){
						if (part > currNoPartitions && mSiteLoad.get(part) > 0) currNoPartitions = part;
					}
					
					currentPlan = algo.computePlan(hotTuplesList, mSiteLoad, "test.txt", Provisioning.noOfSitesRequiredQuery(client, currNoPartitions));
					currentPlan.toJSON("test.txt");

						if(connectedHost == null){
						    connectToHost();
						}
 						ClientResponse cresponse = null;
						try {
						    cresponse = client.callProcedure("@Reconfiguration", 0, "test1.txt", "livepull");
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
	
	public void connectToHost(){
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


	

	/**
	 * @param args
	 */
	public static void main(String[] vargs) throws Exception{
		if(vargs.length == 0){
			System.out.println("Must specify server hostname");
			return;
		}		
        ArgumentsParser args = ArgumentsParser.load(vargs,
			        ArgumentsParser.PARAM_CATALOG);

        Controller c = new Controller(args.catalog);
       	c.run();
	}
}
