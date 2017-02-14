package org.qcri.affinityplanner;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.sysprocs.Reconfiguration;
import org.qcri.affinityplanner.ReconfigurationPredictor.Move;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;


/**
 * Created by mserafini on 2/14/17.
 */
public class PredictiveController {
    private org.voltdb.client.Client m_client;
    private Collection<Site> m_sites;
    private CatalogContext m_catalog_context;
    private String m_connectedHost;
    private ReconfigurationPredictor m_planner;

    public static boolean EXEC_MONITORING = true;
    public static boolean EXEC_UPDATE_PLAN = true;
    public static boolean EXEC_RECONF = true;

    public static int MAX_PARTITIONS;
    public static int PARTITIONS_PER_SITE;
    public static String PLAN_IN = "plan_affinity.json";
    public static String PLAN_OUT = "plan_out.json";

    public static double MAX_CAPACITY_PER_PART = Double.MAX_VALUE;
    public static int DB_MIGRATION_TIME = Integer.MAX_VALUE;

    public static int MONITORING_TIME = 1000;

    public void PredictiveController(Catalog catalog, HStoreConf hstore_conf, CatalogContext catalog_context) {
        if(EXEC_MONITORING || EXEC_RECONF){
            m_client = ClientFactory.createClient();
            m_client.configureBlocking(false);
        }
        m_sites = CatalogUtil.getAllSites(catalog);
        m_catalog_context = catalog_context;

        TreeSet<Integer> partitionIds = new TreeSet<>();
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

        m_planner = new ReconfigurationPredictor(MAX_CAPACITY_PER_PART, PARTITIONS_PER_SITE, DB_MIGRATION_TIME);

        if (hstore_conf.global.hasher_plan != null) {
            PLAN_IN = hstore_conf.global.hasher_plan;
//            LOG.info("Updating plan_in to be " + PLAN_IN);
        }
        else{
            System.out.println("No plan specified. Exiting");
            System.exit(1);
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

    }

    public void run () {
        if(EXEC_MONITORING || EXEC_RECONF) {
            connectToHost();
        }

        File planFile = new File (PLAN_IN);
        Path[] logFiles = new Path[MAX_PARTITIONS];
        Path[] intervalFiles = new Path[MAX_PARTITIONS];
        for (int i = 0; i < MAX_PARTITIONS; i++){
            logFiles[i] = FileSystems.getDefault().getPath(".", "transactions-partition-" + i + ".log");
            intervalFiles[i] = FileSystems.getDefault().getPath(".", "interval-partition-" + i + ".log");
        }

        if(EXEC_MONITORING) {
            String[] confNames = {"site.txn_counters"};
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

            while (true){
                try {
                    cresponse = m_client.callProcedure("@Statistics", "TXNCOUNTER", 0);
                } catch (IOException | ProcCallException e) {
                    record("Problem while turning on monitoring");
                    record(stackTraceToString(e));
                    System.exit(1);
                }

                // TODO it should be possible to run monitoring, planning and reconfigurations separately
                // TODO the inputs and outputs of these steps should be serialized to a file

                // extract transaction times per partition and pass it to the predictor
                // for the moment just print these times

                ArrayList<Long> loads = new ArrayList<>();
                // TODO ArrayList<Long> loads = predictor (latest_data);

                // launch predictor

                ArrayList<Double> predicted_load = new ArrayList<>();
                // TODO ArrayList<Double> predicted_load = predictor(loads);

                // detect active partitions
                IntList activePartitions = new IntArrayList(Controller.MAX_PARTITIONS);
                System.out.println("Load per partition before reconfiguration");
                for(int i = 0; i < Controller.MAX_PARTITIONS; i++){
                    long load = loads.get(i);
                    if(load > 0){
                        activePartitions.add(i);
                        System.out.println(i + ": " + load);
                    }
                }

                // launch planner and get the moves
                ArrayList<Move> moves = m_planner.bestMoves(predicted_load, activePartitions.size());

                // invoke squall at the right times according to the moves
                for (Move move : moves){
                    // TODO convert move.time into a sleep time
                    // TODO obtain a plan that uses move.partitions
                    // TODO sleep
                    reconfig();
                }

                try {
                    Thread.sleep(MONITORING_TIME);
                } catch (InterruptedException e) {
                    record("sleeping interrupted while monitoring");
                    System.exit(1);
                }
            }
        }
    }

    public static void record(String s){
        System.out.println(s);
        FileUtil.appendEventToFile(s);
    }

    private void connectToHost(){
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

    private void reconfig() {
        ClientResponse cresponse = null;
        try {
            // TODO should compare with PLAN_IN before reconfiguring
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

        if (cresponse.getStatus() != Hstoreservice.Status.OK) {
            record("@Reconfiguration transaction aborted");
            System.exit(1);
        }
    }


    /**
     * @param vargs
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
