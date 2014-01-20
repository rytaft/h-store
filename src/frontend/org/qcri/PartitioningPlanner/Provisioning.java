package org.qcri.PartitioningPlanner;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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
	private final double CPU_THRESHOLD_UP;
	private final double CPU_THRESHOLD_DOWN;
	private final int SITES_PER_HOST;
	private final int PARTITIONS_PER_SITE;

	Client client;
	HashMap<Integer,Double> CPUUtilPerHost = new HashMap<Integer,Double>();
	private int usedSites;

	public Provisioning(Client client, int partitions, int sitesPerHost, int partitionsPerSite, double highCPU, double lowCPU){
		this.CPU_THRESHOLD_UP = highCPU;
		this.CPU_THRESHOLD_DOWN = lowCPU;
		this.SITES_PER_HOST = sitesPerHost;
		this.PARTITIONS_PER_SITE = partitionsPerSite;
		this.client = client;
		usedSites = (int) Math.floor((double) partitions/ (double) PARTITIONS_PER_SITE); 
		CPUUtilPerHost = queryCPUUtilPerHost(client, usedSites);
	}

	// ======== NOTE!! =================
	// assumes that site 0-7 go to server 0, 8-15 to server 2 and so on
	// and one partition per server
	//
	public int noOfSitesRequiredQuery(){
		float totalUtil = 0;

		for(Map.Entry<Integer, Double> e : CPUUtilPerHost.entrySet()){
			if(e.getKey() < usedSites) totalUtil += e.getValue();
		}
		if(totalUtil/usedSites > CPU_THRESHOLD_UP){
			usedSites += SITES_PER_HOST;
		}
		if(totalUtil/usedSites < CPU_THRESHOLD_DOWN){
			usedSites -= SITES_PER_HOST;
		}
		System.out.println("Provisioning returns " + usedSites + " sites");
		return usedSites;
	}
	
	public boolean needReconfiguration(){
		CPUUtilPerHost = queryCPUUtilPerHost(client, usedSites);
		for(Map.Entry<Integer, Double> e : CPUUtilPerHost.entrySet()){
			int site = e.getKey(); 
			if(site < usedSites){
				double util = e.getValue();
				if (util < CPU_THRESHOLD_DOWN || util > CPU_THRESHOLD_UP) return true;
			}
		}
		return false;
	}

	// TODO not used yet. could be used to tune the CPU threshold dynamically based on SLAs
	public static boolean queryLatencies(Client client){
		//		if(connectedHost == null){
		//			connectToHost();
		//		}
		ClientResponse cresponse = doStatsQuery(client, "TXNRESPONSETIME");

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
	public static ClientResponse doStatsQuery(Client client, String statsType){
		int interval = 0;
		ClientResponse cresponse = null;
		try {
			cresponse = client.callProcedure("@Statistics", statsType, interval);
		} catch (NoConnectionsException e) {
			System.out.println("Controller: lost connection");
			e.printStackTrace();
			System.exit(1);
				
		} catch (IOException e) {
			System.out.println("Controller: IO Exception while connecting to host");
			e.printStackTrace();
			System.exit(1);
		} catch (ProcCallException e) {
			System.out.println("Controller: @Statistics transaction rejected (backpressure?)");
			e.printStackTrace();
			System.exit(1);
		}
		if(cresponse.getStatus() != Status.OK){
			System.out.println("@Statistics transaction aborted");
			System.exit(1);
		}
		return cresponse;
	}
	
	public void refreshCPUStats(){
		CPUUtilPerHost = queryCPUUtilPerHost(client, usedSites);
	}

	private HashMap<Integer, Double> queryCPUUtilPerHost(Client client, int usedSites){
		System.out.println("Used sites: " + usedSites);
		ClientResponse cresponse = doStatsQuery(client, "CPUUSAGE");
		HashMap<Integer,Double> res = new HashMap<Integer,Double>();
		try{
			Thread.sleep(3000);
		} catch (InterruptedException e) {}
		cresponse = doStatsQuery(client, "CPUUSAGE");
		VoltTable stats = cresponse.getResults()[0];
		for(int r = 0; r < stats.getRowCount(); ++r){
			VoltTableRow row = stats.fetchRow(r);
			System.out.println("Examining site " +  row.getString(2));
			int site = (int) row.getLong(1);
			int host = site / SITES_PER_HOST;
			System.out.println("Host is " + host);
			double utilization = row.getDouble(4);
			System.out.println("Utilization is " + utilization);
			res.put(host,utilization);
		}
		return res;
	}
	
// ======== THE FOLLOWING METHODS USE SSH AND PS TO GET CPU UTILIZATION =================
	
//	public static int noOfSitesRequiredSSH(Collection<Site> sites, int usedSites){
//		double cpuUsage = 0;
//		for(Site site : sites){
//			if(site.getId() % SITES_PER_HOST == 0 && site.getId() < usedSites){
//				String ip = site.getHost().getIpaddr();
//				cpuUsage += getCPUSsh(ip);
//			}
//		}
//		if ((cpuUsage / usedSites) > CPU_THRESHOLD_UP){
//			// currently adds only one server at a time
//			return usedSites + SITES_PER_HOST;
//		}
//		if ((cpuUsage / usedSites) < CPU_THRESHOLD_DOWN){
//			return usedSites - SITES_PER_HOST;
//		}
//		return usedSites;
//	}
	
//	public static double getCPUSsh(String ip){
//	System.out.println("Polling " + ip);
//	String results = ShellTools.cmd("ssh " + ip + " ps -eo pcpu");
//	//        String results = ShellTools.cmd("ssh " + ip + " ls -l");
//	String[] lines = results.split("\n");
//	if (lines.length < 2){
//		System.out.println("Controller: Problem while polling CPU usage for host " + ip);
//		System.exit(-1);
//	}
//	double cpuUsage = 0;
//	for (int i = 1; i < lines.length; ++i){
//		try{
//			cpuUsage += Double.parseDouble(lines[i]);
//		} catch(NumberFormatException e){
//			System.out.println("Problem reading CPU utilization from ip " + ip);
//			System.out.println("ps returned the following response. It could not be parsed");
//			System.out.println(results);
//			e.printStackTrace();
//			System.exit(-1);
//		}
//	}
//	System.out.println("CPU usage of host " + ip + ": " + cpuUsage);
//	return cpuUsage;
//}
}
