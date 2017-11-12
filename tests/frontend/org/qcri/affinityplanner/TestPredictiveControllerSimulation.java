package org.qcri.affinityplanner;

import edu.brown.BaseTestCase;
import edu.brown.utils.FileUtil;

import org.voltdb.client.*;
import org.qcri.affinityplanner.ReconfigurationPredictor.Move;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * Created by rytaft on 11/5/17.
 */
public class TestPredictiveControllerSimulation extends BaseTestCase {
    private org.voltdb.client.Client m_client;
    private ReconfigurationPredictor m_planner;
    private LinkedList<SquallMove> m_next_moves;
    private ParallelMigration m_migration;
    private long m_next_moves_time = 0;
    private boolean m_stop = false;
    private ArrayList<Long> m_eff_cap;

    private static int PARTITIONS_PER_SITE = 6;
    private static int NUM_SITES = 100;

    // Prediction variables
    private ConcurrentLinkedQueue<Long> m_historyNLoads;
    private ArrayList<Long> m_predictedLoad;
    
    // Time in milliseconds for collecting historical load data and making a prediction:
    private static int MONITORING_TIME = 30000;  //(e.g. 3000 ms = 3 sec = 30 sec B2W-time)
    
    private static int FUDGE_FACTOR = 1;
    private static long MAX_CAPACITY_PER_SERVER_PER_SEC = (long) Math.ceil(438 * FUDGE_FACTOR); // Q=350 txns/s
    private static long MAX_CAPACITY_PER_SERVER = (long) Math.ceil(285 * FUDGE_FACTOR * MONITORING_TIME/1000.0); // Q=350 txns/s
    //public static long MAX_CAPACITY_PER_SERVER = (long) Math.ceil(23000 * FUDGE_FACTOR * MONITORING_TIME/1000.0); // Q=26000 txns/s
    private static int DB_MIGRATION_TIME = (int) Math.ceil(4646 * 1000.0/MONITORING_TIME); // D=4224 seconds + 10% buffer
    //public static int DB_MIGRATION_TIME = (int) Math.ceil(1000 * 1000.0/MONITORING_TIME); // D=908 seconds + 10% buffer
    private static int MAX_MOVES_STALENESS = 30000; // time in milliseconds before moves are considered invalid
    private static int POLL_TIME = 1000;
    private static double PREDICTION_INFLATION = 1.15; // inflate predictions by 15%

    private static String ORACLE_PREDICTION_FILE = "/data/rytaft/oracle_prediction_2016_07_01.txt";
    private static boolean USE_ORACLE_PREDICTION = false;
    private static boolean REACTIVE_ONLY = false;
    private static boolean REMOVE_TINY_RECONFS = true;
    private static long SCALE_IN_WAIT = 3 * MONITORING_TIME;

    //String PREDICTION_FILE = "/data/rytaft/predpoints_forecastwindow_60_train_once.txt";
    String PREDICTION_FILE = "/data/rytaft/predpoints_forecastwindow_60_retrain1month.txt";
    String ACTUAL_LOAD_FILE = "/data/rytaft/actual_load.txt";

    private class SquallMove {
        protected long start_time;
        protected int nodes;
        protected int time_interval;

        public SquallMove(long start_time, int nodes, int time_interval){
            this.start_time = start_time;
            this.nodes = nodes;
            this.time_interval = time_interval;
        }
        
        @Override
        public String toString() {
            return "<Nodes: " + nodes + ", Time Interval: " + time_interval + ", Start Time: " + start_time + ">;";
        }
    }
    
    private ArrayList<Long> getEffCap() {
        return this.m_eff_cap;
    }

    public TestPredictiveControllerSimulation() {
        m_client = ClientFactory.createClient();
        m_client.configureBlocking(false);
        m_historyNLoads = new ConcurrentLinkedQueue<>();
        m_predictedLoad = new ArrayList<>();
        m_eff_cap = new ArrayList<>();
        
        m_migration = new ParallelMigration(PARTITIONS_PER_SITE, DB_MIGRATION_TIME);
        m_planner = new ReconfigurationPredictor(MAX_CAPACITY_PER_SERVER, m_migration);

    }

    private void runSimulation () throws Exception {

        boolean oraclePredictionComplete = false;
        SquallMove next_move = null;
        int activeSites = 9;
        Long scalein_requested_time = null;
        long currentTime = 0;
        int numPredictions = 0;

        try {
            File file = new File(PREDICTION_FILE);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);

            while (!m_stop){
                if (((m_next_moves != null && !m_next_moves.isEmpty()) || next_move != null)
                        && (numPredictions * MONITORING_TIME >= currentTime
                                || USE_ORACLE_PREDICTION || REACTIVE_ONLY)){
                    if (next_move == null) {
                        next_move = m_next_moves.pop();
                    }
                    long sleep_time = next_move.start_time - currentTime;
                    if (sleep_time > 0){
                        scalein_requested_time = null;
                        if (sleep_time > POLL_TIME) {
                            //record("Simulating sleeping for " + POLL_TIME + " ms - waiting on start of move");
                            currentTime += POLL_TIME;
                            continue;
                        } else {
                            //record("Simulating sleeping for " + sleep_time + " ms - waiting on start of move");
                            currentTime += sleep_time;
                        }                    
                    }

                    // Don't scale in too quickly or you might need to scale out again right away
                    if (next_move.nodes < activeSites) { 
                        if (scalein_requested_time == null) {
                            scalein_requested_time = currentTime;
                        } 
                        if (currentTime - scalein_requested_time < SCALE_IN_WAIT ) {
                            //record("Simulating sleeping for " + POLL_TIME + " ms - scale in wait");
                            currentTime += POLL_TIME;
                            continue;
                        }
                    }

                    scalein_requested_time = null;
                    while (m_eff_cap.size() <= currentTime/1000) {
                        m_eff_cap.add(activeSites * MAX_CAPACITY_PER_SERVER_PER_SEC);
                    }                   
                        
                    int duration = m_migration.reconfigTime(activeSites, next_move.nodes) * MONITORING_TIME;
                    record("Starting reconfiguration from " + activeSites + " to " + next_move.nodes + " nodes");
                    record("Simulating reconfiguration lasting " + duration + " ms");
                    currentTime += duration;
                    for (int i = 1; i <= duration/1000; ++i) {
                        m_eff_cap.add((long) m_planner.effectiveCapacity(i, duration/1000, activeSites, next_move.nodes));
                    }
                    
                    activeSites = next_move.nodes;
                    next_move = null;      
                }
                else if (USE_ORACLE_PREDICTION) {
                    if (oraclePredictionComplete) {
                        record("Oracle Prediction Complete");
                        System.exit(0);
                    }

                    ArrayList<Long> predictedLoad = new ArrayList<>();
                    try {
                        File f = new File(ORACLE_PREDICTION_FILE);
                        FileReader ofr = new FileReader(f);
                        BufferedReader obr = new BufferedReader(ofr);

                        String line = obr.readLine();
                        while (line != null) {
                            predictedLoad.add((long) (Long.parseLong(line) * PREDICTION_INFLATION));
                            line = obr.readLine();
                        } 

                        obr.close();
                    } catch (IOException e) {
                        record("Unable to read predicted load");
                        record(stackTraceToString(e));
                        //System.exit(1);
                    }

                    // launch planner and get the moves
                    ArrayList<Move> moves = m_planner.bestMoves(predictedLoad, activeSites);
                    if(moves == null || moves.isEmpty()){
                        // reactive migration
                        record("Initiating reactive migration to " + m_planner.getMaxNodes() + " nodes");
                        m_next_moves = convert(m_planner.getMaxNodes(), activeSites, currentTime);
                    }
                    else {
                        //record("Moves: " + moves.toString());
                        m_next_moves = convert(moves, activeSites, currentTime);
                    }
                    next_move = null;
                    m_next_moves_time = currentTime;

                    oraclePredictionComplete = true;
                }
                else if (REACTIVE_ONLY){

                    // prepare a plan that increases or reduces the number of servers if needed
                    Long[] historyNLoads = m_historyNLoads.toArray(new Long[]{});
                    long load1 = 0;
                    long load2 = 0;
                    long load3 = 0;

                    if(historyNLoads.length > 0) load1 = (long) (historyNLoads[historyNLoads.length-1] * PREDICTION_INFLATION);
                    if(historyNLoads.length > 1) load2 = (long) (historyNLoads[historyNLoads.length-2] * PREDICTION_INFLATION);
                    if(historyNLoads.length > 2) load3 = (long) (historyNLoads[historyNLoads.length-3] * PREDICTION_INFLATION);

                    if (load1 == 0 && load2 == 0 && load3 == 0) continue;

                    if(activeSites > 1 && load1 < MAX_CAPACITY_PER_SERVER * (activeSites - 1) &&
                            load2 < MAX_CAPACITY_PER_SERVER * (activeSites - 1) &&
                            load3 < MAX_CAPACITY_PER_SERVER * (activeSites - 1)) {
                        record("Initiating reactive migration to " + (activeSites - 1) + " nodes");
                        m_next_moves = convert(activeSites - 1, activeSites, currentTime);
                        next_move = null;
                        //                } else if (total > 0 && (double) count_gt_50 / total > 0.10) {
                    } else if (load1 > MAX_CAPACITY_PER_SERVER * activeSites &&
                            load2 > MAX_CAPACITY_PER_SERVER * activeSites &&
                            load3 > MAX_CAPACITY_PER_SERVER * activeSites) {
                        record("Initiating reactive migration to " + (activeSites + 1) + " nodes");
                        m_next_moves = convert(activeSites + 1, activeSites, currentTime);
                        next_move = null;
                    } else {
                        //record("Simulating sleeping for " + POLL_TIME + " ms");
                        currentTime += POLL_TIME;
                    }
                }
                else {
                    // launch predictor
                    try {
                        if (m_predictedLoad.isEmpty() || (numPredictions * MONITORING_TIME) < currentTime) {
                            String line = br.readLine();
                            numPredictions++;
                            while (line != null && (numPredictions * MONITORING_TIME) < currentTime) {
                                line = br.readLine();
                                numPredictions++;
                            }
                            record("currentTime: " + currentTime + ", numPredictions: " + numPredictions);
                            if (line != null) {
                                m_predictedLoad.clear();
                                String[] predictedLoadStr = line.split(" ");
                                for (String s : predictedLoadStr) {
                                    m_predictedLoad.add((long) (Double.parseDouble(s) * PREDICTION_INFLATION));
                                }
                                
                                //System.out.println(">> Predictions: ");
                                //for (int i = 0; i < m_predictedLoad.size(); i++) {
                                //    if (i != m_predictedLoad.size() - 1) {
                                //        System.out.print(m_predictedLoad.get(i) + ",");
                                //    } else {
                                //        System.out.println(m_predictedLoad.get(i));
                                //    }
                                //}
                                
                                // launch planner and get the moves
                                ArrayList<Move> moves = m_planner.bestMoves(m_predictedLoad, activeSites);
                                if (moves == null || moves.isEmpty()) {
                                    // reactive migration
                                    record("Initiating reactive migration to " + m_planner.getMaxNodes() + " nodes");
                                    m_next_moves = convert(m_planner.getMaxNodes(), activeSites, currentTime);
                                } else {
                                    //record("Moves: " + moves.toString());
                                    m_next_moves = convert(moves, activeSites, currentTime);
                                }
                                next_move = null;
                                if (m_next_moves.size() == 0) scalein_requested_time = null;
                                m_next_moves_time = currentTime;
                                
                            } else {
                                m_stop = true;
                            }
                        }
                    } catch (IOException e) {
                        System.out.println("Unable to read predicted load");
                        e.printStackTrace();
                        System.exit(1);
                    }

                    if (m_next_moves.size() == 0) {
                        //record("Simulating sleeping for " + POLL_TIME + " ms - no predicted moves");
                        currentTime += POLL_TIME;
                    }
                }            
            }
            while (m_eff_cap.size() <= currentTime/1000) {
                m_eff_cap.add(activeSites * MAX_CAPACITY_PER_SERVER_PER_SEC);
            }
            br.close();
        } catch (IOException e) {
            System.out.println("Unable to read predicted load");
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void record(String s){
        System.out.println(s);
        FileUtil.appendEventToFile(s);
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

    private LinkedList<SquallMove> convert(ArrayList<Move> moves, int activeSites, long currentTime){
        LinkedList<SquallMove> squallMoves = new LinkedList<>();
        long moveStart = currentTime;
        int prevNodes = activeSites;
        String debug = "";
        if (REMOVE_TINY_RECONFS) {
            moves = removeTinyReconfs(moves);
        }
        
        for (Move move : moves) {
            if (move.nodes > NUM_SITES) {
                record("ERROR: required number of nodes (" + move.nodes + ") exceeds number of sites (" + NUM_SITES + ")");
                move.nodes = NUM_SITES;
            }
            
            // skip no-op moves
            if (move.nodes != prevNodes)  {
                SquallMove squallMove = new SquallMove(moveStart, move.nodes, move.time);
                squallMoves.add(squallMove);
                debug += squallMove.toString();
            }
            
            // calculate the start time for the next move, and save current number of nodes
            moveStart = move.time * MONITORING_TIME + currentTime;
            prevNodes = move.nodes;
        }
        record("Squall moves: " + debug);
        return squallMoves;
    }
    
    private LinkedList<SquallMove> convert(int nodes, int activeSites, long currentTime){
        LinkedList<SquallMove> squallMoves = new LinkedList<>();
        if (nodes > NUM_SITES) {
            record("ERROR: required number of nodes (" + nodes + ") exceeds number of sites (" + NUM_SITES + ")");
            nodes = NUM_SITES;
        }
        
        // skip no-op moves
        if (nodes == activeSites) return squallMoves;
        
        squallMoves.add(new SquallMove(currentTime, nodes, -1));
        return squallMoves;
    }

    private static String stackTraceToString(Throwable e){
        // alternatively can write StdErr to file
        // System.setErr(new PrintStream("error.log"));
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
    
    private int compareToActualLoad(ArrayList<Long> effCap) {
        ArrayList<Long> actualLoad = new ArrayList<>();
        try {
            File f = new File(ACTUAL_LOAD_FILE);
            FileReader fr = new FileReader(f);
            BufferedReader br = new BufferedReader(fr);

            String line = br.readLine();
            while (line != null) {
                actualLoad.add(Long.parseLong(line));
                line = br.readLine();
            } 

            br.close();
        } catch (IOException e) {
            record("Unable to read predicted load");
            record(stackTraceToString(e));
            //System.exit(1);
        }
        
        int secondsAboveCap = 0;
        System.out.println("Actual load size: " + actualLoad.size());
        System.out.println("Effective capacity size: " + effCap.size());
        
        for(int i = 0; i < actualLoad.size() && i < effCap.size(); ++i) {
            if (actualLoad.get(i) > effCap.get(i)) {
                secondsAboveCap++;
            }
        }
        
        return secondsAboveCap;
    }
    
    public void testBestMovesRealLoadSimulation() throws Exception {
        record("Running the predictive controller simulation");

        TestPredictiveControllerSimulation c = new TestPredictiveControllerSimulation();
        try {
            c.runSimulation();
        } catch (Exception e) {
            e.printStackTrace();
            record("Not good");
            return;
        }
        
        System.out.println("Effective capacity: " + c.getEffCap());
        System.out.println("Seconds above capacity: " + compareToActualLoad(c.getEffCap()));
    }



}
