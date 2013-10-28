package org.qcri.ehstore;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;

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
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;

public class Controller implements Runnable {
	
	private static Client client;
	private static String connectedHost;
	private static Catalog catalog;
	private static Collection<Site> sites;
	private static final double CPU_THRESHOLD = 400;
		
	// used HStoreTerminal as model to handle the catalog
	public Controller (Catalog cat){
		// connect to VoltDB server
        client = ClientFactory.createClient();
        client.configureBlocking(false);
        catalog = cat;
        sites = CatalogUtil.getAllSites(catalog);
        Site catalog_site = CollectionUtil.random(CatalogUtil.getAllSites(catalog));
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
	}

	@Override
	public void run () {
        System.out.println("Accessing host " + connectedHost + " at port " + HStoreConstants.DEFAULT_PORT);
    	
		try {

	        String statsType = "TXNRESPONSETIME";
	        int interval = 0;
	        System.out.println("Connected, gettings stats!");
	        ClientResponse cresponse = null;
	        while(true) {
				try {
					cresponse = client.callProcedure("@Statistics", statsType, interval);
				} catch (NoConnectionsException e) {
					System.out.println("Controller: lost connection");
					e.printStackTrace();
					getOverloadedSites(); // TODO call the bin packer here and pass it the list of sites
					continue;
				} catch (IOException e) {
					System.out.println("Controller: IO Exception while connecting to host");
					e.printStackTrace();
					getOverloadedSites();
					continue;
				} catch (ProcCallException e) {
					System.out.println("Controller: @Statistics transaction rejected (backpressure?)");
					e.printStackTrace();
					getOverloadedSites();
					continue;
				}
	 
				if(cresponse.getStatus() != Status.OK){
					System.out.println("@Statistics transaction aborted");
					getOverloadedSites();
					continue;
				}
				
	        	System.out.println("Received stats, parsing...");
	        	VoltTable stats = cresponse.getResults()[0];
	        	
	        	if(isRegular(stats)){
	        		System.out.println("Everything allright");
	        	}
	        	else{
	        		System.out.println("Problem detected");
	        		getOverloadedSites();
	        	}
	        
				Thread.sleep(1000);
	        }
		} catch (InterruptedException e1) {
			e1.printStackTrace();
			return;
		}
	}
	
	private boolean isRegular(VoltTable stats){
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
	
	private static Collection<Site> getOverloadedSites() throws InterruptedException{
		System.out.println("Controller: detecting overloaded sites");
		ArrayList<Site> res = new ArrayList<Site> (sites.size());
		for(Site s : sites){
			String ip = s.getHost().getIpaddr();
	        String command = String.format("ssh " + ip + " 'ps -o pcpu' ");
	        String results = ShellTools.cmd(command);
	        Thread.sleep(1000);
	        String[] lines = results.split("\n");
	        if (lines.length < 2){
	        	System.out.println("Controller: Problem while polling CPU usage for host");
	        	System.exit(-1);
	        }
	        double cpuUsage = 0;
	        for (int i = 1; i < lines.length; ++i){
	        	cpuUsage += Double.parseDouble(lines[i]);
	        }
	        if (cpuUsage > CPU_THRESHOLD){
	        	res.add(s);
	        }
		}
		return res;
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
