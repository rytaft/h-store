package org.qcri.ehstore;

import java.io.IOException;
import java.net.UnknownHostException;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;

public class Controller {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
        // connect to VoltDB server
        Client client = ClientFactory.createClient();
        HStoreConf hStoreConf = HStoreConf.singleton();
        String host = hStoreConf.get("global.defaulthost").toString();
        int j;
        
        System.out.println("Accessing host " + host + " at port " + HStoreConstants.DEFAULT_PORT);
        	
        try {
			client.createConnection(null, host, HStoreConstants.DEFAULT_PORT, "user", "password");
		} catch (UnknownHostException e) {
			System.out.println("Controller: tried to connect to unknown host");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Controller: IO Exception while connecting to host");
			e.printStackTrace();
		}
        
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
			} catch (IOException e) {
				System.out.println("Controller: IO Exception while connecting to host");
				e.printStackTrace();
			} catch (ProcCallException e) {
				System.out.println("Controller: Error while invoking @Statistics");
				e.printStackTrace();
			}
 
			if(cresponse.getStatus() != Status.OK){
				System.out.println("@Statistics transactions aborted");
				continue;
			}
			
        	System.out.println("Received stats, parsing...");
        	VoltTable [] memStats = cresponse.getResults();
        
        	for(j = 0; j < memStats.length; ++j) {
        		System.out.println(memStats[j].toString());
        	}
        	try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				System.out.println("Controller: Interrupted while sleeping");
				e.printStackTrace();
			}
        }

	}

}
