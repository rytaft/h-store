package org.qcri.affinityplanner;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;

import org.hsqldb.lib.*;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.client.*;
import org.voltdb.processtools.ShellTools;
import org.json.JSONException;
import org.qcri.affinityplanner.ReconfigurationPredictor.Move;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by mserafini on 2/14/17.
 */
public class PredictiveController {
    private org.voltdb.client.Client m_client;
    private Collection<Site> m_sites;
    private ReconfigurationPredictor m_planner;
    private Predictor m_predictor = new Predictor();
    private LinkedList<SquallMove> m_next_moves;
    private long m_next_moves_time = 0;
    private boolean m_stop = false;

    public static int RECONFIG_LEADER_SITEID = 0;

    public static int MAX_PARTITIONS;
    public static int PARTITIONS_PER_SITE;
    public static String PLAN_IN = "plan_affinity.json";

    // Prediction variables
    public static String LOAD_HIST_PRED = "agg_load_hist_preds.csv";
    public static String LOAD_HIST = "agg_load_hist.csv";
    public static int N_HISTORICAL_OBS = 30;
    public ConcurrentLinkedQueue<Long> m_historyNLoads = new ConcurrentLinkedQueue<>();
    
    // Reaction variables
    public AtomicLong m_count_lt_20 = new AtomicLong();
    public AtomicLong m_count_lt_50 = new AtomicLong();
    public AtomicLong m_count_gt_50 = new AtomicLong();

    // The following 3 parameters need to be consistent with each other:
    // (1) Time in milliseconds for collecting historical load data and making a prediction:
    public static int MONITORING_TIME = 30000;  //(e.g. 3000 ms = 3 sec = 30 sec B2W-time)
    // (2) Number of data points to predict into the future 
    public static int NUM_PREDS_AHEAD = 26;  // (e.g. for MONITORING_TIME=3000, to predict 1hour => NUM_PREDS_AHEAD = 120 pts)
    // (3) Fitted model coefficients, based on (1) rate [Temporarily hard-coded] 
    //public static String MODEL_COEFFS_FILE = "/home/nosayba/h-store/src/frontend/org/qcri/affinityplanner/prediction_model_coeffs.txt";
    public static String MODEL_COEFFS_FILE = "/data/nosayba/prediction_model_coeffs.txt";
    public static String FASTFWD_FILE = "/data/nosayba/fastforward_load_60samples.txt";
    public static boolean USE_FAST_FORWARD = false;
    
    public static int FUDGE_FACTOR = 2;
    public static long MAX_CAPACITY_PER_SERVER = (long) Math.ceil(285 * FUDGE_FACTOR * MONITORING_TIME/1000.0); // Q=350 txns/s
    public static int DB_MIGRATION_TIME = (int) Math.ceil(4646 * 1000.0/MONITORING_TIME); // D=4224 seconds + 10% buffer
    public static int MAX_MOVES_STALENESS = 5000; // time in milliseconds before moves are considered invalid
    public static int POLL_TIME = 1000;
    public static double PREDICTION_INFLATION = 1.1; // inflate predictions by 10%

    public static String ORACLE_PREDICTION_FILE = "/data/rytaft/oracle_prediction_2016_07_01.txt";
    public static boolean USE_ORACLE_PREDICTION = false;
    public static boolean REACTIVE_ONLY = false;
    public static boolean REMOVE_TINY_RECONFS = true;
    public static long SCALE_IN_WAIT = 2 * MONITORING_TIME;

    private class MonitorThread implements Runnable {

        private org.voltdb.client.Client client;
        private long[] previousLoads;
        private long[] currLoads;
        ConcurrentLinkedQueue<Long> historyNLoads;
        AtomicLong m_count_lt_20;
        AtomicLong m_count_lt_50;
        AtomicLong m_count_gt_50;
        File loadHistoryFile;

        public MonitorThread(ConcurrentLinkedQueue<Long> historyNLoads, Collection<Site> sites,
                AtomicLong count_lt_20, AtomicLong count_lt_50, AtomicLong count_gt_50) {
            client = ClientFactory.createClient();
            client.configureBlocking(false);
            connectToHost(client,sites);

            previousLoads = new long[sites.size()];
            currLoads = new long[sites.size()];
            this.historyNLoads = historyNLoads;
            this.m_count_lt_20 = count_lt_20;
            this.m_count_lt_50 = count_lt_50;
            this.m_count_gt_50 = count_gt_50;
            
            loadHistoryFile = new File(LOAD_HIST);
        }
        
        private long extractLoad() {
            ClientResponse cresponse = null;
            try {
                cresponse = client.callProcedure("@Statistics", "TXNCOUNTER", 0);
            } catch (IOException | ProcCallException e) {
                record("Problem while turning on monitoring");
                record(stackTraceToString(e));
                System.exit(1);
            }

            // extract total load
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
                //record("hostid=" + hostId + " -- procedure=" + procedure + " -- load=" + load);
            }

            long totalLoad = 0;
            for (int i = 0; i < currLoads.length; i++) {
                totalLoad += (currLoads[i] - previousLoads[i]);
            }

            long [] swap = previousLoads;
            previousLoads = currLoads;
            currLoads = swap;
            for (int i = 0; i < currLoads.length; i++){
                currLoads[i] = 0;
            }                

            // For debugging purposes
            record(" >> totalLoad =" + totalLoad);
            
            return totalLoad;
        }

        @Override
        public void run() {

            if(USE_FAST_FORWARD){
                String fastFwdSamples = FileUtil.readFile(LOAD_HIST);
                // First check that there aren't existing historical predictions
                if (fastFwdSamples.isEmpty()) {
                    // Else read from fastfwd file
                    record("Reading load from " + FASTFWD_FILE);
                    fastFwdSamples = FileUtil.readFile(FASTFWD_FILE);
                    try {
                        FileUtil.appendStringToFile(loadHistoryFile, fastFwdSamples);
                    } catch (IOException e) {
                        record("Problem logging historical load");
                        e.printStackTrace();
                    }
                } else {
                    record("Reading load from " + LOAD_HIST);
                }                               

                // Add fast-forwarded sample points to history log  
                String[] fwdSamples = fastFwdSamples.split("\n");
                for (int i = 0; i < fwdSamples.length; i++) {
                    historyNLoads.add(Long.valueOf( fwdSamples[i] ));
                } 
            }
            
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
            
            // Run extractLoad once since the first load value is really high
            extractLoad();
            
            while(!m_stop) {
                try {
                    Thread.sleep(MONITORING_TIME);
                } catch (InterruptedException e) {
                    record("sleeping interrupted while monitoring");
                    System.exit(1);
                }

                long totalLoad = extractLoad();
                
                if(totalLoad != 0 ){
                    historyNLoads.add(totalLoad);
                    try {
                        FileUtil.appendStringToFile(loadHistoryFile, new String(totalLoad + "\n"));
                    } catch (IOException e) {
                        record("Problem logging historical load");
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public class SquallMove {
        public String new_plan;
        public long start_time;
        public int nodes;
        public int time_interval;

        public SquallMove(String new_plan, long start_time, int nodes, int time_interval){
            this.new_plan = new_plan;
            this.start_time = start_time;
            this.nodes = nodes;
            this.time_interval = time_interval;
        }
        
        @Override
        public String toString() {
            return "<Nodes: " + nodes + ", Time Interval: " + time_interval + ", Start Time: " + start_time + ">;";
        }
    }

    public PredictiveController(Catalog catalog, HStoreConf hstore_conf) {
        m_client = ClientFactory.createClient();
        m_client.configureBlocking(false);
        m_sites = CatalogUtil.getAllSites(catalog);
        m_historyNLoads = new ConcurrentLinkedQueue<>();
        m_count_lt_20 = new AtomicLong();
        m_count_lt_50 = new AtomicLong();
        m_count_gt_50 = new AtomicLong();
        
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

        m_planner = new ReconfigurationPredictor(MAX_CAPACITY_PER_SERVER, new ParallelMigration(PARTITIONS_PER_SITE, DB_MIGRATION_TIME));

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

    public void run () throws Exception {

        connectToHost(m_client, m_sites);

        resetLogs(); // necessary to detect ongoing reconfigurations

        // TODO use currnt plan file for conversion of moves
        File planFile = new File (PLAN_IN);
        String currentPlan = FileUtil.readFile(planFile);

        File loadHistFile = new File(LOAD_HIST_PRED);
        
        boolean oraclePredictionComplete = false;
        SquallMove next_move = null;
        int activeSites = countActiveSites(planFile.toString());
        Long scalein_requested_time = null;

        Thread monitor = new Thread(new MonitorThread(m_historyNLoads, m_sites, 
                m_count_lt_20, m_count_lt_50, m_count_gt_50));
        if(!USE_ORACLE_PREDICTION) {
            monitor.start();
        }

        while (!m_stop){
            if(isReconfigurationRunning()){
                try {
                    Thread.sleep(POLL_TIME);
                } catch (InterruptedException e) {
                    record("sleeping interrupted while waiting for reconfiguration");
                    System.exit(1);
                }
                continue;
            }
            else if (m_next_moves != null && !m_next_moves.isEmpty()
                    && (System.currentTimeMillis() - m_next_moves_time < MAX_MOVES_STALENESS
                            || USE_ORACLE_PREDICTION || REACTIVE_ONLY)){
                if (next_move == null) {
                    next_move = m_next_moves.pop();
                }
                long sleep_time = next_move.start_time - System.currentTimeMillis();
                if (sleep_time > 0){
                    try {
                        if (sleep_time > POLL_TIME) {
                            record("Sleeping for " + POLL_TIME + " ms");
                            Thread.sleep(POLL_TIME);
                            continue;
                        } else {
                            record("Sleeping for " + sleep_time + " ms");
                            Thread.sleep(sleep_time);
                        }
                    } catch (InterruptedException e) {
                        record("sleeping interrupted while waiting for next move");
                        System.exit(1);
                    }                    
                }
                
                // Don't scale in too quickly or you might need to scale out again right away
                if (next_move.nodes < activeSites) { 
                    if (scalein_requested_time == null) {
                        scalein_requested_time = System.currentTimeMillis();
                    } 
                    if (System.currentTimeMillis() - scalein_requested_time < SCALE_IN_WAIT ) {
                        try {
                            Thread.sleep(POLL_TIME);
                        } catch (InterruptedException e) {
                            record("sleeping interrupted while waiting for reconfiguration");
                            System.exit(1);
                        }
                        continue;
                    } else {
                        scalein_requested_time = null;
                    }
                }

                currentPlan = next_move.new_plan;
                activeSites = next_move.nodes;
                record("Starting reconfiguration to " + activeSites + " nodes");
                record("Moving to plan: " + currentPlan);
                reconfig(currentPlan);
                next_move = null;

                try {
                    FileUtil.writeStringToFile(planFile, currentPlan);
                } catch (IOException e) {
                    record("Unable to write new plan file");
                    record(stackTraceToString(e));
                    System.exit(1);
                }
            }
            else if (USE_ORACLE_PREDICTION) {
                if (oraclePredictionComplete) {
                    record("Oracle Prediction Complete");
                    System.exit(0);
                }
                
                ArrayList<Long> predictedLoad = new ArrayList<>();
                try {
                    File file = new File(ORACLE_PREDICTION_FILE);
                    FileReader fr = new FileReader(file);
                    BufferedReader br = new BufferedReader(fr);

                    String line = br.readLine();
                    while (line != null) {
                        predictedLoad.add((long) (Long.parseLong(line) * PREDICTION_INFLATION));
                        line = br.readLine();
                    } 
                    
                    br.close();
                } catch (IOException e) {
                    record("Unable to read predicted load");
                    record(stackTraceToString(e));
                    System.exit(1);
                }
                
                // launch planner and get the moves
                ArrayList<Move> moves = m_planner.bestMoves(predictedLoad, activeSites);
                if(moves == null || moves.isEmpty()){
                    // reactive migration
                    record("Initiating reactive migration to " + m_planner.getMaxNodes() + " nodes");
                    m_next_moves = convert(currentPlan, m_planner.getMaxNodes());
                }
                else {
                    record("Moves: " + moves.toString());
                    insertDelay(moves);
                    record("Moves with delay: " + moves.toString());
                    m_next_moves = convert(currentPlan, moves, activeSites);
                }
                next_move = null;
                m_next_moves_time = System.currentTimeMillis();

                oraclePredictionComplete = true;
            }
            else if (REACTIVE_ONLY){
                
                // prepare a plan that increases or reduces the number of servers if needed
                // Latency stuff doesnt really work...
//                long count_lt_20 = m_count_lt_20.get();
//                long count_lt_50 = m_count_lt_50.get();
//                long count_gt_50 = m_count_gt_50.get();
//                long total = count_lt_20 + count_lt_50 + count_gt_50;
//                if (total > 0 && (double) count_lt_20 / total > 0.95) {
                Long[] historyNLoads = m_historyNLoads.toArray(new Long[]{});
                long load1 = 0;
                long load2 = 0;
                long load3 = 0;
                
                if(historyNLoads.length > 0) load1 = historyNLoads[historyNLoads.length-1];
                if(historyNLoads.length > 1) load2 = historyNLoads[historyNLoads.length-2];
                if(historyNLoads.length > 2) load3 = historyNLoads[historyNLoads.length-3];
                
                if (load1 == 0 && load2 == 0 && load3 == 0) continue;
                
                if(activeSites > 1 && load1 < MAX_CAPACITY_PER_SERVER * (activeSites - 1) &&
                        load2 < MAX_CAPACITY_PER_SERVER * (activeSites - 1) &&
                        load3 < MAX_CAPACITY_PER_SERVER * (activeSites - 1)) {
                    record("Initiating reactive migration to " + (activeSites - 1) + " nodes");
                    m_next_moves = convert(currentPlan, activeSites - 1);
                    next_move = null;
//                } else if (total > 0 && (double) count_gt_50 / total > 0.10) {
                } else if (load1 > MAX_CAPACITY_PER_SERVER * activeSites &&
                        load2 > MAX_CAPACITY_PER_SERVER * activeSites &&
                        load3 > MAX_CAPACITY_PER_SERVER * activeSites) {
                    record("Initiating reactive migration to " + (activeSites + 1) + " nodes");
                    m_next_moves = convert(currentPlan, activeSites + 1);
                    next_move = null;
                } else {
                    try {
                        Thread.sleep(POLL_TIME);
                    } catch (InterruptedException e) {
                        record("sleeping interrupted");
                        System.exit(1);
                    }
                }
            }
            else {

                // TODO for debugging, it should be possible to run monitoring, planning and reconfigurations separately
                // TODO the inputs and outputs of these steps should be serialized to a file

                // launch predictor
                ArrayList<Long> predictedLoad = m_predictor.predictLoad(m_historyNLoads, NUM_PREDS_AHEAD, MODEL_COEFFS_FILE);
                if (predictedLoad == null) {
                    try {
                        Thread.sleep(POLL_TIME);
                    } catch (InterruptedException e) {
                        record("sleeping interrupted while waiting for predictions");
                        System.exit(1);
                    }
                    continue;
                }
                
                for (int i = 0; i < predictedLoad.size(); i++) {
                    predictedLoad.set(i, (long) (predictedLoad.get(i) * PREDICTION_INFLATION));
                }

                if (predictedLoad != null) {
                    System.out.println(">> Predictions: ");
                    //System.out.println(predictedLoad.toString());
                    //System.out.println();

                    //System.out.print(totalLoad + ",");
                    for (int i = 0; i < predictedLoad.size(); i++) {
                        if (i != predictedLoad.size() - 1) {
                            System.out.print(predictedLoad.get(i) + ",");
                        } else {
                            System.out.println(predictedLoad.get(i));
                        }
                    }

                    try {
                        FileUtil.appendStringToFile(loadHistFile, m_predictor.lastTotalLoad + ",");
                        for (int i = 0; i < predictedLoad.size(); i++) {
                            if (i != predictedLoad.size() - 1) {
                                FileUtil.appendStringToFile(loadHistFile, predictedLoad.get(i) + ",");
                            } else {
                                FileUtil.appendStringToFile(loadHistFile, predictedLoad.get(i) + "\n");
                            }
                        }
                    } catch (IOException e) {
                        record("Problem logging load/predictions");
                        e.printStackTrace();
                    }

                    // launch planner and get the moves
                    ArrayList<Move> moves = m_planner.bestMoves(predictedLoad, activeSites);
                    if (moves == null || moves.isEmpty()) {
                        // reactive migration
                        record("Initiating reactive migration to " + m_planner.getMaxNodes() + " nodes");
                        m_next_moves = convert(currentPlan, m_planner.getMaxNodes());
                    } else {
                        record("Moves: " + moves.toString());
                        m_next_moves = convert(currentPlan, moves, activeSites);
                    }
                    next_move = null;
                    m_next_moves_time = System.currentTimeMillis();
                }
            }
        }
    }


    public void stop(){
        m_stop = true;
    }
    
    // HACK to handle 3.9 minute delay between 10 minute runs of the benchmark
    private void insertDelay(ArrayList<Move> moves) {
        for(Move move : moves) {
            int fast_forward = (int) Math.round((move.time / (600000/MONITORING_TIME)) * (3.9125 * 60000/MONITORING_TIME));
            move.time += fast_forward;
        }       
    }

    public static void record(String s){
        System.out.println(s);
        FileUtil.appendEventToFile(s);
    }

    private void connectToHost(Client client, Collection<Site> sites){
        Site catalog_site = CollectionUtil.random(sites);
        String connectedHost= catalog_site.getHost().getIpaddr();

        try {
            client.createConnection(null, connectedHost, HStoreConstants.DEFAULT_PORT, "user", "password");
        } catch (UnknownHostException e) {
            record("Controller: tried to connect to unknown host");
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            record("Controller: IO Exception while connecting to host");
            e.printStackTrace();
            System.exit(1);
        }
        record("Connected to host " + connectedHost);
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
    
    private ArrayList<Move> removeTinyReconfs(ArrayList<Move> moves) {
        ArrayList<Move> filteredMoves = new ArrayList<>();
        Move prevMove = null;
        Move prevPrevMove = null;
        for(Move move : moves) {
            if (prevMove != null) {
                if (prevPrevMove == null) {
                    filteredMoves.add(prevMove);
                } else if (move.nodes != prevPrevMove.nodes || move.nodes <= prevMove.nodes) {
                    filteredMoves.add(prevMove);
                }
            }
            
            prevPrevMove = prevMove;
            prevMove = move;
        }
        filteredMoves.add(prevMove);
        return filteredMoves;
    }

    private LinkedList<SquallMove> convert(String plan, ArrayList<Move> moves, int activeSites){
        LinkedList<SquallMove> squallMoves = new LinkedList<>();
        long currentTime = System.currentTimeMillis();
        long moveStart = currentTime;
        int prevNodes = activeSites;
        String debug = "";
        if (REMOVE_TINY_RECONFS) {
            moves = removeTinyReconfs(moves);
        }
        
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
                    SquallMove squallMove = new SquallMove(plan, moveStart, move.nodes, move.time);
                    squallMoves.add(squallMove);
                    debug += squallMove.toString();
                } catch (JSONException e) {
                    record("ERROR: Failed to convert plan to string " + e.getMessage());
                }               
            }
            
            // calculate the start time for the next move, and save current number of nodes
            moveStart = move.time * MONITORING_TIME + currentTime;
            prevNodes = move.nodes;
        }
        record("Squall moves: " + debug);
        return squallMoves;
    }
    
    private LinkedList<SquallMove> convert(String plan, int nodes){
        LinkedList<SquallMove> squallMoves = new LinkedList<>();
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
        squallMoves.add(new SquallMove(plan, System.currentTimeMillis(), nodes, -1));
        return squallMoves;
    }

    private void resetLogs(){
        System.out.println("Resetting logs");
        String hStoreDir = ShellTools.cmd("pwd");
        hStoreDir = hStoreDir.replaceAll("(\\r|\\n)", "");
        String command = "python scripts/partitioning/reset_hevent.py " + hStoreDir;
        for(Site site: m_sites){
            command = command + " " + site.getHost().getIpaddr();
        }
        @SuppressWarnings("unused")
        String results = ShellTools.cmd(command);
        System.out.println("Done resetting logs");
    }

    private boolean isReconfigurationRunning() throws Exception {
        String reconfLeaderIp = null;

        String hStoreDir = ShellTools.cmd("pwd");
        hStoreDir = hStoreDir.replaceAll("(\\r|\\n)", "");
        String command = "python scripts/partitioning/fetch_hevent.py " + hStoreDir;
        for(Site site: m_sites){
            command = command + " " + site.getHost().getIpaddr();
            if(site.getId() == RECONFIG_LEADER_SITEID){
                reconfLeaderIp = site.getHost().getIpaddr();
            }
        }
        @SuppressWarnings("unused")
        String results = ShellTools.cmd(command);

        Path logFile = FileSystems.getDefault().getPath(".", "hevent-" + reconfLeaderIp + ".log"); // TODO works only if controller is launched from site0
        BufferedReader reader;
        try {
            reader = Files.newBufferedReader(logFile, Charset.forName("US-ASCII"));
        } catch (IOException e) {
            Controller.record("Error while reading file " + logFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
            throw e;
        }

        String line;
        try {
            line = reader.readLine();
        } catch (IOException e) {
            Controller.record("Error while reading file " + logFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
            throw e;
        }
        if (line == null){
            Controller.record("File " + logFile.toString() + " is empty");
        }

        boolean reconfiguring = false;
        String startReconf = "LEADER_RECONFIG_INIT";
        String endReconf = "RECONFIGURATION_END";

        while(line != null){
            if (line.contains(startReconf)){
                reconfiguring = true;
            }
            else if (line.contains(endReconf)){
                reconfiguring = false;
            }
            try {
                line = reader.readLine();
            } catch (IOException e) {
                Controller.record("Error while reading file " + logFile.toString() + "\n Stack trace:\n" + Controller.stackTraceToString(e));
                throw e;
            }
        }

        reader.close();
        return reconfiguring;
    }

    private int countActiveSites(String currentPlanFile){
        Plan plan = new Plan(currentPlanFile);
        int maxActivePartitionCount = plan.getMaxActivePartition() + 1;
        return (int) Math.ceil((double) maxActivePartitionCount / (double) PARTITIONS_PER_SITE);
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

        PredictiveController c = new PredictiveController(args.catalog, hstore_conf);
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
