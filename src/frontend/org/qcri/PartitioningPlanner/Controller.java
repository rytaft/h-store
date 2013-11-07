package org.qcri.PartitioningPlanner;

import java.io.IOException;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.HashMap;


import org.qcri.PartitioningPlanner.placement.Placement;
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
		
	private static final double CPU_THRESHOLD = 0.8;
	private static final int PARTITIONS_PER_HOST = 8;
	private static final int POLL_FREQUENCY = 3000;

	// used HStoreTerminal as model to handle the catalog
	public Controller (Catalog catalog){
		algo = new Placement();
		// connect to VoltDB server
        client = ClientFactory.createClient();
        client.configureBlocking(false);
        sites = CatalogUtil.getAllSites(catalog);
        
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
    	while(true){
			try {
				List<Site> overloaded = getOverloadedSites();
				if (overloaded != null && !overloaded.isEmpty()){

					//Jennie temp for now
					Map<Integer, Integer> hotTuples, siteLoad;
					hotTuples = new HashMap<Integer, Integer>();
					siteLoad = new HashMap<Integer, Integer>();
					currentPlan = algo.computePlan(hotTuples, siteLoad, currentPlan);
					try {
						currentPlan.toJSON(planFile);
					} catch(Exception e) {
						System.out.println("Caught on exception " + e.toString());
					}
				}
				Thread.sleep(POLL_FREQUENCY);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
				return;
			}
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
        ArgumentsParser args = ArgumentsParser.load(vargs,
			        ArgumentsParser.PARAM_CATALOG);

        Controller c = new Controller(args.catalog);
       	c.run();
	}
}
