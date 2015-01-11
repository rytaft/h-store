package org.qcri.affinityplanner;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;

import org.voltdb.CatalogContext;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.processtools.ShellTools;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;

public class Controller {
    private org.voltdb.client.Client m_client;
    private Collection<Site> m_sites;
    private CatalogContext m_catalog_context;
    private String m_connectedHost;
    
//    private Path planFile;
//    private Path outputPlanFile;
    
    public static int MAX_PARTITIONS;
    public static int PARTITIONS_PER_SITE;

    public static int MONITORING_TIME = 20000;
    public static boolean RUN_MONITORING = true;
    public static boolean UPDATE_PLAN = true;
    public static boolean EXEC_RECONF = true;
    public static String PLAN_IN = "plan.json";
    public static String PLAN_OUT = "plan_out.json";
    
    public Controller (Catalog catalog, HStoreConf hstore_conf, CatalogContext catalog_context) {
        m_client = ClientFactory.createClient();
        m_client.configureBlocking(false);
        m_sites = CatalogUtil.getAllSites(catalog);
        m_catalog_context = catalog_context;
        
        TreeSet<Integer> partitionIds = new TreeSet<Integer>();
        MAX_PARTITIONS = -1;
        PARTITIONS_PER_SITE = -1;
        for(Site site: m_sites){
            for (Partition part: site.getPartitions()){
                int id = part.getId();
                partitionIds.add(id);
                if (id > MAX_PARTITIONS){
                    MAX_PARTITIONS = id;
                }
            }
            // verify that all sites have the same number of partitions
            if (PARTITIONS_PER_SITE == -1){
                PARTITIONS_PER_SITE = site.getPartitions().size();
            }
            else if (PARTITIONS_PER_SITE != site.getPartitions().size()){
                System.out.println("Not all sites have the same number of partitions. Exiting");
                System.exit(1);
            }
        }
        MAX_PARTITIONS++;
        
        // verify that all partition ids are contiguous
        Iterator<Integer> partIdsIt = partitionIds.iterator();
        int lastPart = partIdsIt.next();
        while (partIdsIt.hasNext()){
            int currPart = partIdsIt.next();
            if (currPart == (lastPart + 1)){
                lastPart = currPart;
            }
            else{
                System.out.println("Gap in partition ids. Exiting");
                System.exit(1);
            }
        }

//        if(hstore_conf.global.hasher_plan == null){
//            System.out.println("Must set global.hasher_plan to specify plan file!");
//            System.out.println("Using default (plan.json)");
//            planFile = FileSystems.getDefault().getPath("plan.json");
//
//        }
//        else{
//            planFile = FileSystems.getDefault().getPath(hstore_conf.global.hasher_plan);
//        }
//
//        outputPlanFile = FileSystems.getDefault().getPath("plan_out.json");
//
//        try {
//            Files.copy(planFile, outputPlanFile, StandardCopyOption.REPLACE_EXISTING);              
//        }
//        catch(IOException e) {
//            System.out.println("Controller: IO Exception while copying plan file to output plan file");
//            e.printStackTrace();
//        }

        //TODO select planners here
    }
    
    public void run (){
        // turn monitoring on and off
        if(RUN_MONITORING || EXEC_RECONF){
            connectToHost();
        }

        if(RUN_MONITORING){
            String[] confNames = {"site.access_tracking"};
            String[] confValues = {"true"};
            @SuppressWarnings("unused")
            ClientResponse cresponse;
            try {
                cresponse = m_client.callProcedure("@SetConfiguration", confNames, confValues);
            } catch (IOException | ProcCallException e) {
                System.out.println("Problem while turning on monitoring");
                System.out.println(stackTraceToString(e));
                System.exit(1);
            }
            try {
                Thread.sleep(MONITORING_TIME);
            } catch (InterruptedException e) {
                System.out.println("sleeping interrupted while monitoring");
                System.exit(1);
            }
            confValues[0] = "false";
            try {
                cresponse = m_client.callProcedure("@SetConfiguration", confNames, confValues);
            } catch (IOException | ProcCallException e) {
                System.out.println("Problem while turning off");
                System.out.println(stackTraceToString(e));
                System.exit(1);
            }
        } // END if(RUN_MONITORING)
        
        if(UPDATE_PLAN){
            // fetch remote monitoring outputs
            String hStoreDir = ShellTools.cmd("pwd");
            hStoreDir = hStoreDir.replaceAll("(\\r|\\n)", "");
            for(Site site: m_sites){
                String ip = site.getHost().getIpaddr();
                for (int i = 0; i < MAX_PARTITIONS; i++){
                    String command = "scp " + ip + ":" + hStoreDir + "/transactions-partition-" + i + ".log .";
    //                System.out.println("Executing command:\n" + command);
                    @SuppressWarnings("unused")
                    String results = ShellTools.cmd(command);
    //                System.out.println("Result:\n" + results);
                    command = "scp " + ip + ":" + hStoreDir + "/transactions-partition-" + i + "-interval.log .";
    //                System.out.println("Executing command:\n" + command);
                    results = ShellTools.cmd(command);
    //                System.out.println("Result:\n" + results);
                }
            }
    
            File planFile = new File (PLAN_IN);
            Path[] logFiles = new Path[MAX_PARTITIONS];
            Path[] intervalFiles = new Path[MAX_PARTITIONS];
            for (int i = 0; i < MAX_PARTITIONS; i++){
                logFiles[i] = FileSystems.getDefault().getPath(".", "transactions-partition-" + i + ".log");
                intervalFiles[i] = FileSystems.getDefault().getPath(".", "transactions-partition-" + i + "-interval.log");
            }
    
            AffinityGraph graph = new AffinityGraph();
            graph.loadFromFiles(m_catalog_context, planFile, logFiles, intervalFiles, MAX_PARTITIONS);
            Path graphFile = FileSystems.getDefault().getPath(".", "graph.log");
            graph.toFileDebug(graphFile);
            
            GraphPartitioner partitioner = new GraphPartitioner(graph, planFile, m_catalog_context);
            partitioner.repartition();
            partitioner.writePlan(PLAN_OUT);
 
            String outputPlan = FileUtil.readFile(PLAN_OUT);
            System.out.println("Output plan\n" + outputPlan);

            System.out.println("Loads per partition");
            for (int j = 0; j < graph.getPartitionsNo(); j++){
                System.out.println(j + " " + partitioner.getLoadPerPartition(j));
            }
            
            
        } // END if(UPDATE_PLAN)

        if(EXEC_RECONF){
            ClientResponse cresponse = null;
            try {
                String outputPlan = FileUtil.readFile(PLAN_OUT);
                cresponse = m_client.callProcedure("@ReconfigurationRemote", 0, outputPlan, "livepull");
                                //cresponse = client.callProcedure("@ReconfigurationRemote", 0, outputPlan, "stopcopy");
                                System.out.println("Controller: received response: " + cresponse);
            } catch (NoConnectionsException e) {
                System.out.println("Controller: lost connection");
                e.printStackTrace();
                System.exit(1);
            } catch (IOException e) {
                System.out.println("Controller: IO Exception while connecting to host");
                e.printStackTrace();
                System.exit(1);
            } catch (ProcCallException e) {
                System.out.println("Controller: @Reconfiguration transaction rejected (backpressure?)");
                e.printStackTrace();
                System.exit(1);
            }

            if(cresponse.getStatus() != Status.OK){
                System.out.println("@Reconfiguration transaction aborted");
                System.exit(1);
            }
        } // END if(EXEC_RECONF)

        
    }
    
    public void connectToHost(){
        Site catalog_site = CollectionUtil.random(m_sites);
        m_connectedHost= catalog_site.getHost().getIpaddr();

        try {
            m_client.createConnection(null, m_connectedHost, HStoreConstants.DEFAULT_PORT, "user", "password");
        } catch (UnknownHostException e) {
            System.out.println("Controller: tried to connect to unknown host");
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            System.out.println("Controller: IO Exception while connecting to host");
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("Connected to host " + m_connectedHost);
    }
    
    /**
     * @param args
     */
    public static void main(String[] vargs){
        
        if(vargs.length == 0){
            System.out.println("Must specify server hostname");
            return;
        }       

        ArgumentsParser args = null;
        try {
            args = ArgumentsParser.load(vargs,ArgumentsParser.PARAM_CATALOG);
        } catch (Exception e) {
            System.out.println("Problem while parsing Controller arguments");
            System.out.println(stackTraceToString(e));
            return;
        }
        
        HStoreConf hstore_conf = HStoreConf.initArgumentsParser(args);

        //System.out.println("Params: bench"+params[0] +" no. part " +vargs[1] + " twin "+vargs[2]+" plannerID "+vargs[3]);
        System.out.println("args:: "+args.toString());

        System.out.println("vargs.length: "+vargs.length);

        Controller c = new Controller(args.catalog, hstore_conf, args.catalogContext);
        try {
            c.run();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Not good");
            return;
        }
    }
    
    public static String stackTraceToString(Throwable e){
        // alternatively can write StdErr to file
        // System.setErr(new PrintStream("error.log"));
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
}
