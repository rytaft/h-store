package org.qcri.PartitioningPlanner;

import java.io.IOException;
import java.util.Collection;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.processtools.ShellTools;

import edu.brown.hstore.Hstoreservice.Status;

public class Provisioning {
	
	// thresholds are per site
	private static final double CPU_THRESHOLD_UP = 150;
	private static final double CPU_THRESHOLD_DOWN = 100;
	private static final int SITES_PER_HOST = 2;
	
	// ======== NOTE!! =================
	// assumes that site 0-7 go to server 0, 8-15 to server 2 and so on
	// and one partition per server
	//
	// also assumes that currPartitions is a multuple of SITES_PER_HOST
	public static int noOfSitesRequiredSSH(Collection<Site> sites, int usedSites){
		double cpuUsage = 0;
		for(Site site : sites){
			if(site.getId() % SITES_PER_HOST == 0 && site.getId() < usedSites){
				String ip = site.getHost().getIpaddr();
				cpuUsage += getCPUSsh(ip);
			}
		}
		if ((cpuUsage / usedSites) > CPU_THRESHOLD_UP){
			// currently adds only one server at a time
			return usedSites + SITES_PER_HOST;
		}
		if ((cpuUsage / usedSites) < CPU_THRESHOLD_DOWN){
			return usedSites - SITES_PER_HOST;
		}
		return usedSites;
	}
	
	// ======== NOTE!! =================
	// assumes that site 0-7 go to server 0, 8-15 to server 2 and so on
	// and one partition per server
	//
	public static int noOfSitesRequiredQuery(Client client, int usedSites){
		float totalUtil = 0;
		
        ClientResponse cresponse = doQuery(client, "CPUUtil");
    	VoltTable stats = cresponse.getResults()[0];
    	for(int r = 0; r < stats.getRowCount(); ++r){
    		VoltTableRow row = stats.fetchRow(r);
    		int host = (int) (row.getLong(1) % (long) SITES_PER_HOST);
    		if(host < usedSites) totalUtil += row.getDouble(4);
    	}
   		if(totalUtil/usedSites > CPU_THRESHOLD_UP){
   			return usedSites + SITES_PER_HOST;
   		}
   		if(totalUtil/usedSites < CPU_THRESHOLD_DOWN){
   			return usedSites - SITES_PER_HOST;
   		}
    	return usedSites;
	}
	
	public static double getCPUSsh(String ip){
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
        	try{
        		cpuUsage += Double.parseDouble(lines[i]);
        	} catch(NumberFormatException e){
        		System.out.println("Problem reading CPU utilization from ip " + ip);
        		System.out.println("ps returned the following response. It could not be parsed");
        		System.out.println(results);
        		e.printStackTrace();
        		System.exit(-1);
        	}
        }
        System.out.println("CPU usage of host " + ip + ": " + cpuUsage);
        return cpuUsage;
	}
	
	// TODO not used yet. could be used to tune the CPU threshold dynamically based on SLAs
	public static boolean queryLatencies(Client client){
//		if(connectedHost == null){
//			connectToHost();
//		}
        ClientResponse cresponse = doQuery(client, "TXNRESPONSETIME");
		
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
	
	// general method to execute queries
	public static ClientResponse doQuery(Client client, String statsType){
        int interval = 0;
        ClientResponse cresponse = null;
		try {
			cresponse = client.callProcedure("@Statistics", statsType, interval);
		} catch (NoConnectionsException e) {
			System.out.println("Controller: lost connection");
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			System.out.println("Controller: IO Exception while connecting to host");
			e.printStackTrace();
			return null;
		} catch (ProcCallException e) {
			System.out.println("Controller: @Statistics transaction rejected (backpressure?)");
			e.printStackTrace();
			return null;
		}
		if(cresponse.getStatus() != Status.OK){
			System.out.println("@Statistics transaction aborted");
			return null;
		}
		return cresponse;
	}
}
