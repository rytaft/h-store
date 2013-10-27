package org.qcri.ehstore;

import java.io.IOException;
import java.net.UnknownHostException;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Site;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;

public class Controller implements Runnable {
	
	private static Client client;
	private static String host;
	private static Catalog catalog;
		
	public Controller (Catalog cat){
		// connect to VoltDB server
        client = ClientFactory.createClient();
        catalog = cat;
        Site catalog_site = CollectionUtil.random(CatalogUtil.getAllSites(this.catalog));
        host= catalog_site.getHost().getIpaddr();

        try {
			client.createConnection(null, host, HStoreConstants.DEFAULT_PORT, "user", "password");
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
	public void run (){
        System.out.println("Accessing host " + host + " at port " + HStoreConstants.DEFAULT_PORT);
    	

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
				return;
			} catch (IOException e) {
				System.out.println("Controller: IO Exception while connecting to host");
				e.printStackTrace();
				return;
			} catch (ProcCallException e) {
				System.out.println("Controller: Error while invoking @Statistics");
				e.printStackTrace();
				return;
			}
 
			if(cresponse.getStatus() != Status.OK){
				System.out.println("@Statistics transactions aborted");
				continue;
			}
			
        	System.out.println("Received stats, parsing...");
        	VoltTable [] memStats = cresponse.getResults();
        
        	for(int j = 0; j < memStats.length; ++j) {
        		System.out.println(memStats[j].toString());
        	}
        	try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				System.out.println("Controller: Interrupted while sleeping");
				e.printStackTrace();
				return;
			}
        }
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
