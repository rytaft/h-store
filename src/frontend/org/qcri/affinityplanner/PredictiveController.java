package org.qcri.affinityplanner;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.Hstoreservice;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntBigLists;
import it.unimi.dsi.fastutil.ints.IntList;

import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.sysprocs.Reconfiguration;
import org.json.JSONException;
import org.qcri.affinityplanner.ReconfigurationPredictor.Move;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.*;


/**
 * Created by mserafini on 2/14/17.
 */
public class PredictiveController {
    private org.voltdb.client.Client m_client;
    private Collection<Site> m_sites;
    private CatalogContext m_catalog_context;
    private String m_connectedHost;
    private ReconfigurationPredictor m_planner;
    private long[] m_previousLoads;
    private Predictor m_predictor = new Predictor();
    private LinkedList<SquallMove> m_next_moves;

    public static boolean EXEC_MONITORING = true;
    public static boolean EXEC_UPDATE_PLAN = true;
    public static boolean EXEC_RECONF = true;

    public static int MAX_PARTITIONS;
    public static int PARTITIONS_PER_SITE;
    public static String PLAN_IN = "plan_affinity.json";

    public static long MAX_CAPACITY_PER_PART = 3110L; //Long.MAX_VALUE;
    public static int DB_MIGRATION_TIME = 6000; //Integer.MAX_VALUE;


    // Prediction variables
    public static String LOAD_HIST = "agg_load_hist_preds.csv";
    public static int N_HISTORICAL_OBS = 30;
    public ArrayList<Long> historyNLoads = new  ArrayList<>();
    public static boolean firstPrediction = true;

    // The following 3 parameters need to be consistent with each other:
    // (1) Time in milliseconds for collecting historical load data and making a prediction:
    public static int MONITORING_TIME = 3000;  //(e.g. 3000 ms = 3 sec = 30 sec B2W-time)
    // (2) Number of data points to predict into the future 
    public static int NUM_PREDS_AHEAD = 2400;  // (e.g. for MONITORING_TIME=3000, to predict 1hour => NUM_PREDS_AHEAD = 120 pts)
    // (3) Fitted model coefficients, based on (1) rate [Temporarily hard-coded] 
    public static String MODEL_COEFFS_FILE = "/home/nosayba/h-store/src/frontend/org/qcri/affinityplanner/prediction_model_coeffs.txt";

    public static String ORACLE_PREDICTION_FILE = "/data/rytaft/oracle_prediction_2016_07_01.txt";
    public static boolean USE_ORACLE_PREDICTION = false;

    public class SquallMove {
        public String new_plan;
        public long start_time;

        public SquallMove(String new_plan, long start_time){
            this.new_plan = new_plan;
            this.start_time = start_time;
        }
    }

    public PredictiveController(Catalog catalog, HStoreConf hstore_conf, CatalogContext catalog_context) {
        if(EXEC_MONITORING || EXEC_RECONF){
            m_client = ClientFactory.createClient();
            m_client.configureBlocking(false);
        }
        m_sites = CatalogUtil.getAllSites(catalog);
        m_catalog_context = catalog_context;
        m_previousLoads = new long[m_sites.size()];

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

        m_planner = new ReconfigurationPredictor(MAX_CAPACITY_PER_PART, new ParallelMigration(PARTITIONS_PER_SITE, DB_MIGRATION_TIME));

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

        connectToHost();

        // TODO use currnt plan file for conversion of moves
        File planFile = new File (PLAN_IN);
        File loadHistFile = new File(LOAD_HIST);

        String[] confNames = {"site.txn_counters"};
        String[] confValues = {"true"};
        @SuppressWarnings("unused")
        ClientResponse cresponse = null;
        try {
            cresponse = m_client.callProcedure("@SetConfiguration", confNames, confValues);
        } catch (IOException | ProcCallException e) {
            record("Problem while turning on monitoring");
            record(stackTraceToString(e));
            System.exit(1);
        }

        while (true){
            try {
                Thread.sleep(MONITORING_TIME);
            } catch (InterruptedException e) {
                record("sleeping interrupted while monitoring");
                System.exit(1);
            }

            if(false /*if reconfiguration is ongoing*/){ // TODO detect that reconfig is ongoing
                continue;
            }
            else if (m_next_moves != null && !m_next_moves.isEmpty()){
                SquallMove next_move = m_next_moves.pop();
                long sleep_time = next_move.start_time - System.currentTimeMillis();
                if (sleep_time > 0){
                    record("Sleeping for " + sleep_time + " ms");
                    try {
                        Thread.sleep(sleep_time);
                    } catch (InterruptedException e) {
                        record("sleeping interrupted while waiting for next move");
                        System.exit(1);
                    }
                }

                record("Moving to plan: " + next_move.new_plan);
                reconfig(next_move.new_plan);

                try {
                    FileUtil.writeStringToFile(planFile, next_move.new_plan);
                } catch (IOException e) {
                    record("Unable to write new plan file");
                    record(stackTraceToString(e));
                    System.exit(1);
                }
            }
            else if (USE_ORACLE_PREDICTION) {
                ArrayList<Long> predictedLoad = new ArrayList<>();
                try {
                    File file = new File(ORACLE_PREDICTION_FILE);
                    FileReader fr = new FileReader(file);
                    BufferedReader br = new BufferedReader(fr);

                    String line = br.readLine();
                    while (line != null) {
                        predictedLoad.add(Long.parseLong(line));
                        line = br.readLine();
                    } 
                    
                    br.close();
                } catch (IOException e) {
                    record("Unable to read predicted load");
                    record(stackTraceToString(e));
                    System.exit(1);
                }
                
                // launch planner and get the moves
                int activeSites = this.m_sites.size(); // TODO get the actual number of active sites
                ArrayList<Move> moves = m_planner.bestMoves(predictedLoad, activeSites);
                if(moves == null || moves.isEmpty()){
                    // reactive migration
                    record("Initiating reactive migration to " + m_planner.getMaxNodes() + " nodes");
                    m_next_moves = convert(planFile, m_planner.getMaxNodes());
                }
                else {
                    record("Moves: " + moves.toString());
                    m_next_moves = convert(planFile, moves, activeSites);
                }

            }
            else {
                try {
                    cresponse = m_client.callProcedure("@Statistics", "TXNCOUNTER", 0);
                } catch (IOException | ProcCallException e) {
                    record("Problem while turning on monitoring");
                    record(stackTraceToString(e));
                    System.exit(1);
                }

                // extract total load and count active sites
                // TODO we might want to count active sites differently?
                long[] currLoads = new long[m_sites.size()];
                int activeSites = 0;
                VoltTable result = cresponse.getResults()[0];
                for (int i = 0; i < result.getRowCount(); i++) {
                    VoltTableRow row = result.fetchRow(i);
                    String procedure = row.getString(3);

                    if (procedure.charAt(0) == '@') {
                        // don't count invocations to system procedures
                        continue;
                    }

                    int hostId = (int) row.getLong(1);

                    // { 4=RECEIVED | 5=REJECTED | 6=REDIRECTED | 7=EXECUTED | 8=COMPLETED }
                    long load = row.getLong(4);

                    currLoads[hostId] = currLoads[hostId] + load;
                    if (hostId > activeSites && load > 0) {
                        activeSites = hostId;
                    }
                    //record("hostid=" + hostId + " -- procedure=" + procedure + " -- load=" + load);
                }
                activeSites++;

                long totalLoad = 0;
                for (int i = 0; i < m_sites.size(); i++) {
                    totalLoad += (currLoads[i] - m_previousLoads[i]);
                }
                m_previousLoads = currLoads;

                // For debugging purposes
                record(" >> totalLoad =" + totalLoad );

                if( firstPrediction ){
                    firstPrediction = false;
                }
                else{
                    historyNLoads.add( (long) totalLoad );

                    // TODO for debugging, it should be possible to run monitoring, planning and reconfigurations separately
                    // TODO the inputs and outputs of these steps should be serialized to a file

                    // launch predictor
                    // TODO implement this method
                    ArrayList<Long> predictedLoad = m_predictor.predictLoad(historyNLoads, NUM_PREDS_AHEAD, MODEL_COEFFS_FILE);

                    if(predictedLoad != null){
                        System.out.println(">> Predictions: ");
                        //System.out.println(predictedLoad.toString());
                        //System.out.println();

                        //System.out.print(totalLoad + ",");
                        for (int i = 0; i < predictedLoad.size(); i++) {
                            if( i != predictedLoad.size() - 1){ System.out.print(predictedLoad.get(i) + ",");}
                            else{ System.out.println(predictedLoad.get(i)); }
                        }

                        try {
                            FileUtil.appendStringToFile(loadHistFile, totalLoad+",");
                            for (int i = 0; i < predictedLoad.size(); i++) {
                                if( i != predictedLoad.size() - 1){ FileUtil.appendStringToFile(loadHistFile,predictedLoad.get(i) + ",");}
                                else{ FileUtil.appendStringToFile(loadHistFile,predictedLoad.get(i) + "\n"); }
                            }
                        } catch (IOException e) {
                            record("Problem logging load/predictions");
                            e.printStackTrace();
                        }

                        // launch planner and get the moves
                        ArrayList<Move> moves = m_planner.bestMoves(predictedLoad, activeSites);
                        if(moves == null || moves.isEmpty()){
                            // reactive migration
                            record("Initiating reactive migration to " + m_planner.getMaxNodes() + " nodes");
                            m_next_moves = convert(planFile, m_planner.getMaxNodes());
                        }
                        else {
                            record("Moves: " + moves.toString());
                            m_next_moves = convert(planFile, moves, activeSites);
                        }

                    }

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

    private void reconfig(String new_plan) {
        ClientResponse cresponse = null;
        try {
            // TODO should compare with PLAN_IN before reconfiguring
            //            String outputPlan = FileUtil.readFile(new_plan);
            //            cresponse = m_client.callProcedure("@ReconfigurationRemote", 0, outputPlan, "livepull");
            cresponse = m_client.callProcedure("@ReconfigurationRemote", 0, new_plan, "livepull");
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

    private LinkedList<SquallMove> convert(File planFile, ArrayList<Move> moves, int activeSites){
        LinkedList<SquallMove> squallMoves = new LinkedList<>();
        String plan = FileUtil.readFile(planFile);
        long currentTime = System.currentTimeMillis();
        long moveStart = currentTime;
        int prevNodes = activeSites;
        for (Move move : moves) {
            if (move.nodes > m_sites.size()) {
                record("ERROR: required number of nodes (" + move.nodes + ") exceeds number of sites (" + m_sites.size() + ")");
                move.nodes = m_sites.size();
            }
            
            // skip no-op moves
            if (move.nodes != prevNodes)  {
                ReconfigurationPlanner planner = new ReconfigurationPlanner(plan, move.nodes * PARTITIONS_PER_SITE, PARTITIONS_PER_SITE);

                planner.repartition();
                try {
                    plan = planner.getPlanString();
                    squallMoves.add(new SquallMove(plan, moveStart));
                } catch (JSONException e) {
                    record("ERROR: Failed to convert plan to string " + e.getMessage());
                }               
            }
            
            // calculate the start time for the next move, and save current number of nodes
            moveStart = move.time * MONITORING_TIME + currentTime;
            prevNodes = move.nodes;
        }
        return squallMoves;
    }
    
    private LinkedList<SquallMove> convert(File planFile, int nodes){
        LinkedList<SquallMove> squallMoves = new LinkedList<>();
        String plan = FileUtil.readFile(planFile);
        if (nodes > m_sites.size()) {
            record("ERROR: required number of nodes (" + nodes + ") exceeds number of sites (" + m_sites.size() + ")");
            nodes = m_sites.size();
        }
        ReconfigurationPlanner planner = new ReconfigurationPlanner(plan, nodes * PARTITIONS_PER_SITE, PARTITIONS_PER_SITE);
        planner.repartition();
        try {
            plan = planner.getPlanString();
        } catch (JSONException e) {
            record("ERROR: Failed to convert plan to string " + e.getMessage());
            return squallMoves;
        }
        squallMoves.add(new SquallMove(plan, System.currentTimeMillis()));
        return squallMoves;
    }


    /**
     * @param vargs
     */
    public static void main(String[] vargs){

        record("Running the predictive controller");

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

        PredictiveController c = new PredictiveController(args.catalog, hstore_conf, args.catalogContext);
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
