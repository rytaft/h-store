package org.qcri.affinityplanner;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.log4j.Logger;
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

public class Controller extends Thread {
    private org.voltdb.client.Client m_client;
    private Collection<Site> m_sites;
    private CatalogContext m_catalog_context;
    private String m_connectedHost;
    
    private static final Logger LOG = Logger.getLogger(Controller.class);

//    private Path planFile;
//    private Path outputPlanFile;
    

    // Controller
    public static int MAX_PARTITIONS;
    public static int PARTITIONS_PER_SITE;

    public static int MONITORING_TIME = 20000;
    public static boolean RUN_MONITORING = true;
    public static boolean UPDATE_PLAN = true;
    public static boolean EXEC_RECONF = true;
    public static String PLAN_IN = "plan_affinity.json";
    public static String PLAN_OUT = "plan_out.json";
    public static String METIS_OUT = "metis.txt";
    public static String METIS_MAP_OUT = "metismap.txt";
    
    
    public static String ALGO = "graph";
    
    // Loader
    public static int LOAD_THREADS = 6;
 
    // Repartitioning
    public static double MIN_LOAD_PER_PART = Double.MIN_VALUE;
    public static double MAX_LOAD_PER_PART = Double.MAX_VALUE;
    public static double LMPT_COST = 1.1;
    public static double DTXN_COST = 5.0;
    public static int MAX_MOVED_TUPLES_PER_PART = 10000;
    public static int MIN_GAIN_MOVE = 0;
    public static int MAX_PARTITIONS_ADDED = 1;
    
    public static int COLD_CHUNK_SIZE = 100;
    public static double COLD_TUPLE_FRACTION_ACCESSES = 100;
    public static int TOPK = Integer.MAX_VALUE;


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
                record("Not all sites have the same number of partitions. Exiting");
                System.exit(1);
            }
        }
        MAX_PARTITIONS++;
        
        if (hstore_conf.global.hasher_plan != null) {
            PLAN_IN = hstore_conf.global.hasher_plan;
            LOG.info("Updating plan_in to be " + PLAN_IN);
        }
        
        // verify that all partition ids are contiguous
        Iterator<Integer> partIdsIt = partitionIds.iterator();
        int lastPart = partIdsIt.next();
        while (partIdsIt.hasNext()){
            int currPart = partIdsIt.next();
            if (currPart == (lastPart + 1)){
                lastPart = currPart;
            }
            else{
                record("Gap in partition ids. Exiting");
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
    
    public static void record(String s){
        System.out.println(s);
        FileUtil.appendEventToFile(s);        
    }
    
    public void run (){
        // turn monitoring on and off
        if(RUN_MONITORING || EXEC_RECONF){
            connectToHost();
        }
        
        long t1;
        long t2;

        File planFile = new File (PLAN_IN);
        Path[] logFiles = new Path[MAX_PARTITIONS];
        Path[] intervalFiles = new Path[MAX_PARTITIONS];
        for (int i = 0; i < MAX_PARTITIONS; i++){
            logFiles[i] = FileSystems.getDefault().getPath(".", "transactions-partition-" + i + ".log");
            intervalFiles[i] = FileSystems.getDefault().getPath(".", "transactions-partition-" + i + "-interval.log");
        }

        if(RUN_MONITORING){
            record("================== RUNNING MONITORING ======================");

            String[] confNames = {"site.access_tracking"};
            String[] confValues = {"true"};
            @SuppressWarnings("unused")
            ClientResponse cresponse;
            try {
                cresponse = m_client.callProcedure("@SetConfiguration", confNames, confValues);
            } catch (IOException | ProcCallException e) {
                record("Problem while turning on monitoring");
                record(stackTraceToString(e));
                System.exit(1);
            }
            try {
                Thread.sleep(MONITORING_TIME);
            } catch (InterruptedException e) {
                record("sleeping interrupted while monitoring");
                System.exit(1);
            }
            confValues[0] = "false";
            try {
                cresponse = m_client.callProcedure("@SetConfiguration", confNames, confValues);
            } catch (IOException | ProcCallException e) {
                record("Problem while turning off");
                record(stackTraceToString(e));
                System.exit(1);
            }

            record("================== FETCHING MONITORING FILES ======================");
            t1 = System.currentTimeMillis();
            String hStoreDir = ShellTools.cmd("pwd");
            hStoreDir = hStoreDir.replaceAll("(\\r|\\n)", "");
            String command = "python scripts/partitioning/fetch_monitor.py " + hStoreDir;
            for(Site site: m_sites){
                command = command + " " + site.getHost().getIpaddr();
                String ip = site.getHost().getIpaddr();
            }
            String results = ShellTools.cmd(command);
    
            t2 = System.currentTimeMillis();
            record("Time taken:" + (t2-t1));

        } // END if(RUN_MONITORING)
        if(UPDATE_PLAN){
            
            record("======================== LOADING GRAPH ========================");
            t1 = System.currentTimeMillis();
            
            Partitioner partitioner = null;
            
            System.out.println("Algorithm " + ALGO);
            
            // checks parameter -D
            switch(ALGO){
                case "simple":
                    partitioner = new SimplePartitioner(m_catalog_context, planFile, logFiles, intervalFiles);
                    break;
                case "graph":
                    partitioner = new GraphGreedy(m_catalog_context, planFile, logFiles, intervalFiles);
                    break;
                case "greedy-ext":
                    partitioner = new GreedyExtended(m_catalog_context, planFile, logFiles, intervalFiles);
                    break;
                default:
                    partitioner = new GraphGreedy(m_catalog_context, planFile, logFiles, intervalFiles);
            }

            t2 = System.currentTimeMillis();
            record("Time taken:" + (t2-t1));
            
            if(partitioner instanceof GreedyExtended){
                LOG.info("Skipping metis out for greedy extended");
            } else {
                LOG.info(String.format("Writing metis graph out to %s ",FileSystems.getDefault().getPath(".", METIS_OUT)));
                Path metisFile = FileSystems.getDefault().getPath(".", METIS_OUT);
                Path metisMapFile = FileSystems.getDefault().getPath(".", METIS_MAP_OUT);
                long start = System.currentTimeMillis();
                partitioner.graphToMetisFile(metisFile,metisMapFile);
                long time = System.currentTimeMillis() - start;
                LOG.info("generating metis out file took : " + time);
                Path metisOut= FileSystems.getDefault().getPath(".",METIS_OUT + ".part." + this.m_catalog_context.numberOfPartitions); 
                String metisExe = String.format("gpmetis %s %s", METIS_OUT, m_catalog_context.numberOfPartitions);
                
                //RESULTS map of hashID -> new partition ID
                Int2IntOpenHashMap metisGeneratedPartitioning;
                try {
                    Path currentRelativePath = Paths.get("");
                    String s = currentRelativePath.toAbsolutePath().toString();
                    LOG.info("Calling metis " + metisExe);
                     start = System.currentTimeMillis();
                    Process metisProc = new ProcessBuilder("gpmetis",METIS_OUT, ""+m_catalog_context.numberOfPartitions).start();
                   // LOG.info("metis proc: " + metisProc.toString());
                    int result = metisProc.waitFor();
                    time = System.currentTimeMillis() - start;
                    if (result == 0){
                        LOG.info(String.format("Metis ran successfully. took : " + time));
                        metisGeneratedPartitioning = getMetisMapping(metisOut, metisMapFile);
                        LOG.info("Results in metis map files: " + metisGeneratedPartitioning.keySet().size());
                    } else {
                        LOG.info(String.format("Metis ran unsuccessfully (%s) : %s", result, metisExe));
                    }
                } catch (Exception e) {
                    LOG.error("Exception running metis", e);
                }
            }
            
//          Path graphFile = FileSystems.getDefault().getPath(".", "graph.log");
//          partitioner.toFileDebug(graphFile);
            
            record("======================== PARTITIONING GRAPH ========================");
            t1 = System.currentTimeMillis();

            boolean b = partitioner.repartition();
            if (!b){
                record("Problem while partitioning graph. Exiting");
                return;
            }
            partitioner.writePlan(PLAN_OUT);
 
            String outputPlan = FileUtil.readFile(PLAN_OUT);
            record("Output plan\n" + outputPlan);
            
            record("Loads per partition after reconfiguration");
            for (int j = 0; j < Controller.MAX_PARTITIONS; j++){
                record(j + " " + partitioner.getLoadPerPartition(j));
            }

            t2 = System.currentTimeMillis();
            record("Time taken:" + (t2-t1));
            record("Partitioner tuples to move: " + Controller.MAX_MOVED_TUPLES_PER_PART);
            
            
        } // END if(UPDATE_PLAN)

        if(EXEC_RECONF){

            record("======================== STARTING RECONFIGURATION ========================");
            ClientResponse cresponse = null;
            try {
                String outputPlan = FileUtil.readFile(PLAN_OUT);
                cresponse = m_client.callProcedure("@ReconfigurationRemote", 0, outputPlan, "livepull");
                                //cresponse = client.callProcedure("@ReconfigurationRemote", 0, outputPlan, "stopcopy");
                                record("Controller: received response: " + cresponse);
            } catch (NoConnectionsException e) {
                record("Controller: lost connection");
                e.printStackTrace();
                System.exit(1);
            } catch (IOException e) {
                record("Controller: IO Exception while connecting to host");
                e.printStackTrace();
                System.exit(1);
            } catch (ProcCallException e) {
                System.out.println("Controller: @Reconfiguration transaction rejected (backpressure?)");
                e.printStackTrace();
                System.exit(1);
            }

            if(cresponse.getStatus() != Status.OK){
                record("@Reconfiguration transaction aborted");
                System.exit(1);
            }
        } // END if(EXEC_RECONF)

        
    }
    
    private Int2IntOpenHashMap getMetisMapping(Path metisOut, Path metisMapFile) {
        LOG.info(String.format("Getting metis out and mapping to hashes for %s and %s", metisOut, metisMapFile));
        Int2IntOpenHashMap res = new Int2IntOpenHashMap();
        BufferedReader outReader, metisMapReader;
        String outPart, hashId;
        try {
            outReader = Files.newBufferedReader(metisOut, Charset.forName("US-ASCII"));
            metisMapReader = Files.newBufferedReader(metisMapFile, Charset.forName("US-ASCII"));
            while(true){
                outPart = outReader.readLine();
                hashId = metisMapReader.readLine();
                if (outPart == null && hashId == null){
                    break;
                } else if (outPart == null && hashId != null){
                    LOG.error("Ran out of hashes before partition maps...");
                    break;
                } else if (outPart == null && hashId == null){
                    LOG.error("Ran out of partition maps before hashes...");
                    break;
                }
                res.put(Integer.parseInt(hashId), Integer.parseInt(outPart));
            }

            
        } catch (IOException e) {
            Controller.record("Error while reading out files \n Stack trace:\n" + Controller.stackTraceToString(e));
        }
        return res;
    }

    public void connectToHost(){
        Site catalog_site = CollectionUtil.random(m_sites);
        m_connectedHost= catalog_site.getHost().getIpaddr();

        try {
            m_client.createConnection(null, m_connectedHost, HStoreConstants.DEFAULT_PORT, "user", "password");
        } catch (UnknownHostException e) {
            record("Controller: tried to connect to unknown host");
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            record("Controller: IO Exception while connecting to host");
            e.printStackTrace();
            System.exit(1);
        }
        record("Connected to host " + m_connectedHost);
    }
    
    /**
     * @param args
     */
    public static void main(String[] vargs){
        
        if(vargs.length == 0){
            record("Must specify server hostname");
            return;
        }       

        ArgumentsParser args = null;
        try {
            args = ArgumentsParser.load(vargs,ArgumentsParser.PARAM_CATALOG);
        } catch (Exception e) {
            record("Problem while parsing Controller arguments");
            record(stackTraceToString(e));
            return;
        }
        
        HStoreConf hstore_conf = HStoreConf.initArgumentsParser(args);

        //System.out.println("Params: bench"+params[0] +" no. part " +vargs[1] + " twin "+vargs[2]+" plannerID "+vargs[3]);
        record("args:: "+args.toString());

        record("vargs.length: "+vargs.length);

        Controller c = new Controller(args.catalog, hstore_conf, args.catalogContext);
        try {
            c.run();
        } catch (Exception e) {
            e.printStackTrace();
            record("Not good");
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
