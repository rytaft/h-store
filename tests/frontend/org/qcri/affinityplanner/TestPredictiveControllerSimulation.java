package org.qcri.affinityplanner;

import edu.brown.BaseTestCase;
import edu.brown.utils.FileUtil;

import org.voltdb.client.*;
import org.qcri.affinityplanner.ReconfigurationPredictor.Move;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
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
    private ReconfigurationPredictor m_tester;
    private LinkedList<SquallMove> m_next_moves;
    private ParallelMigration m_migration;
    private boolean m_stop = false;
    private ArrayList<Long> m_eff_cap;
    private long m_cost = 0;
    private boolean m_use_oracle_prediction = false;
    private boolean m_reactive_only = false;
    private double m_prediction_inflation = 1;
    private double m_prediction_perturbation = 0;
    private String m_prediction_file;

    private static int PARTITIONS_PER_SITE = 6;
    private static int NUM_SITES = 200;

    // Prediction variables
    private ArrayList<Long> m_predictedLoad;
    private ArrayList<Long> m_historicalLoad;
    
    // Time in milliseconds for collecting historical load data and making a prediction:
    private static int MONITORING_TIME = 30000;  //(e.g. 3000 ms = 3 sec = 30 sec B2W-time)
    
    private static int FUDGE_FACTOR = 1;
    private static long MAX_CAPACITY_PER_SERVER_PER_SEC = (long) Math.ceil(438 * FUDGE_FACTOR); // Q=350 txns/s
    private static long MAX_CAPACITY_PER_SERVER = (long) Math.ceil(285 * FUDGE_FACTOR * MONITORING_TIME/1000.0); // Q=350 txns/s
    //public static long MAX_CAPACITY_PER_SERVER = (long) Math.ceil(23000 * FUDGE_FACTOR * MONITORING_TIME/1000.0); // Q=26000 txns/s
    private static int DB_MIGRATION_TIME = (int) Math.ceil(4646 * 1000.0/MONITORING_TIME); // D=4224 seconds + 10% buffer
    //public static int DB_MIGRATION_TIME = (int) Math.ceil(1000 * 1000.0/MONITORING_TIME); // D=908 seconds + 10% buffer
    private static int POLL_TIME = 1000;

    //private static String ORACLE_PREDICTION_FILE = "/data/rytaft/actual_load_5min.txt";
    private static boolean REMOVE_TINY_RECONFS = true;
    private static long SCALE_IN_WAIT = 3 * MONITORING_TIME;

    //private static String PREDICTION_FILE = "/data/rytaft/predpoints_forecastwindow_60_train_once.txt";
    //private static String PREDICTION_FILE = "/data/rytaft/predpoints_forecastwindow_60_retrain1month.txt";
    private static String ACTUAL_LOAD_FILE = "/data/rytaft/actual_load.txt";
    private static String EFF_CAP_FILE = "eff_cap.txt";
    private static String HOURLY_COMPARISON_FILE = "hourly_comparison.txt";

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
    
    private long getCost() {
        return this.m_cost;
    }

    public TestPredictiveControllerSimulation() {
        m_client = ClientFactory.createClient();
        m_client.configureBlocking(false);
        m_predictedLoad = new ArrayList<>();
        m_historicalLoad = new ArrayList<>();
        m_eff_cap = new ArrayList<>();
        m_cost = 0;
        
        m_migration = new ParallelMigration(PARTITIONS_PER_SITE, DB_MIGRATION_TIME);
        m_planner = new ReconfigurationPredictor(MAX_CAPACITY_PER_SERVER, m_migration);
        m_tester = new ReconfigurationPredictor(MAX_CAPACITY_PER_SERVER_PER_SEC, m_migration);

    }

    private void runSimulation (boolean useOraclePrediction, boolean reactiveOnly, double predictionInflation, 
            double predictionPerturbation, String predictionFile, int activeSites) throws Exception {

        boolean oraclePredictionComplete = false;
        SquallMove next_move = null;
        Long scalein_requested_time = null;
        long currentTime = 0;
        int numPredictions = 0;
        int numHistorical = 0;
        m_cost = 0;
        m_use_oracle_prediction = useOraclePrediction;
        m_reactive_only = reactiveOnly;
        m_prediction_inflation = 1 + predictionInflation;
        m_prediction_perturbation = predictionPerturbation;
        m_prediction_file = predictionFile;

        try {
            File file = new File(m_prediction_file);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);

            while (!m_stop){
                if (((m_next_moves != null && !m_next_moves.isEmpty()) || next_move != null)
                        && (numPredictions * MONITORING_TIME >= currentTime
                                || m_use_oracle_prediction || m_reactive_only)){
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
                    if (next_move.nodes < activeSites && !m_use_oracle_prediction) { 
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
                        m_cost += activeSites;
                    }                   
                        
                    int duration = m_migration.reconfigTime(activeSites, next_move.nodes) * MONITORING_TIME;
                    record("Starting reconfiguration from " + activeSites + " to " + next_move.nodes + " nodes");
                    record("Simulating reconfiguration lasting " + duration + " ms");
                    currentTime += duration;
                    for (int i = 1; i <= duration/1000; ++i) {
                        m_eff_cap.add((long) m_tester.effectiveCapacity(i, duration/1000, activeSites, next_move.nodes));
                    }
                    m_cost += m_migration.moveCost(duration/MONITORING_TIME, activeSites, next_move.nodes) * MONITORING_TIME/1000;
                    
                    activeSites = next_move.nodes;
                    next_move = null;      
                }
                else if (m_use_oracle_prediction) {
                    if (oraclePredictionComplete) {
                        record("Oracle Prediction Complete");
                        m_stop = true;
                        break;
                    }

                    ArrayList<Long> predictedLoad = new ArrayList<>();
                    try {
                        File f = new File(m_prediction_file);
                        FileReader ofr = new FileReader(f);
                        BufferedReader obr = new BufferedReader(ofr);

                        String line = obr.readLine();
                        while (line != null) {
                            double perturbation = 1 + (Math.random() * 2 - 1) * m_prediction_perturbation;
                            predictedLoad.add((long) (Long.parseLong(line) * m_prediction_inflation * perturbation));
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
                    //writeHourlyComparison2(predictedLoad, moves);
                    if(moves == null || moves.isEmpty()){
                        // reactive migration
                        record("Initiating reactive migration to " + m_planner.getMaxNodes() + " nodes");
                        m_next_moves = convert(m_planner.getMaxNodes(), activeSites, currentTime);
                    }
                    else {
                        //record("Moves: " + moves.toString());
                        m_next_moves = convert(moves, activeSites, currentTime);
                        //writeHourlyComparison3(predictedLoad, m_next_moves, activeSites, currentTime);
                    }
                    next_move = null;

                    oraclePredictionComplete = true;
                }
                else if (m_reactive_only){

                    if (m_historicalLoad.size() < 3 || ((numHistorical-2) * MONITORING_TIME) < currentTime) {
                        String line = br.readLine();
                        numHistorical++;
                        while (line != null && ((numHistorical-2) * MONITORING_TIME) < currentTime) {
                            if (m_historicalLoad.size() >= 3) m_historicalLoad.remove(0);
                            double perturbation = 1 + (Math.random() * 2 - 1) * m_prediction_perturbation;
                            m_historicalLoad.add((long) (Long.parseLong(line) * m_prediction_inflation * perturbation));
                            line = br.readLine();
                            numHistorical++;
                        }
                        if (line != null) {
                            if (m_historicalLoad.size() >= 3) m_historicalLoad.remove(0);
                            double perturbation = 1 + (Math.random() * 2 - 1) * m_prediction_perturbation;
                            m_historicalLoad.add((long) (Long.parseLong(line) * m_prediction_inflation * perturbation));
                        } else {
                            m_stop = true;
                        }
                        record("currentTime: " + currentTime + ", numHistorical: " + numHistorical);
                    }
                            
                    if (m_historicalLoad.size() >= 3) {
                        // prepare a plan that increases or reduces the number of servers if needed
                        long load1 = m_historicalLoad.get(0);
                        long load2 = m_historicalLoad.get(1);
                        long load3 = m_historicalLoad.get(2);

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
                        
                        if (m_next_moves.size() == 0) {
                            //record("Simulating sleeping for " + POLL_TIME + " ms - no predicted moves");
                            currentTime += POLL_TIME;
                        }
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
                                double perturbation = 1 + (Math.random() * 2 - 1) * m_prediction_perturbation;
                                m_predictedLoad.clear();
                                String[] predictedLoadStr = line.split(" ");
                                boolean first = true;
                                for (String s : predictedLoadStr) {
                                    if (first)
                                        first = false; // skip the first one since that was the load in the past
                                    else
                                        m_predictedLoad.add((long) (Double.parseDouble(s) * m_prediction_inflation * perturbation));
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
                m_cost += activeSites;
            }
            br.close();
        } catch (IOException e) {
            System.out.println("Unable to read predicted load");
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void record(String s){
        //System.out.println(s);
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
        if (REMOVE_TINY_RECONFS && !m_use_oracle_prediction) {
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
    
    private void writeEffCap() {
        try {
            File f = new File(EFF_CAP_FILE);
            FileWriter fw = new FileWriter(f);
            BufferedWriter bw = new BufferedWriter(fw);

            for (Long l : this.getEffCap()) {
                bw.write(l.toString());
                bw.newLine();
            }

            bw.close();
        } catch (IOException e) {
            record("Unable to write effective capacity");
            record(stackTraceToString(e));
            //System.exit(1);
        }
        
    }
    
    private void writeHourlyComparison3(ArrayList<Long> predictedLoad, LinkedList<SquallMove> moves, int activeSites, long currentTime) {
        try {
            ArrayList<Long> effCap = new ArrayList<>();
            long t = currentTime;
            int nodes = activeSites;
            for (SquallMove m : moves) {
                for (; t < m.start_time; t += MONITORING_TIME) {
                    effCap.add(Math.max(m.nodes, nodes) * MAX_CAPACITY_PER_SERVER);
                }
                nodes = m.nodes;
            }
            System.out.println("Size of moves: " + effCap.size());
            
            File f = new File(HOURLY_COMPARISON_FILE);
            FileWriter fw = new FileWriter(f);
            BufferedWriter bw = new BufferedWriter(fw);

            int window = 12;
            bw.write("actualLoad,effCap");
            bw.newLine();
            for (int i = 0; i < predictedLoad.size() && i < effCap.size(); i += window) {
                int actualLoadSum = 0, effCapSum = 0;
                int j = 0;
                for (; j < window && (i+j) < predictedLoad.size() && (i+j) < effCap.size(); ++j) {
                    actualLoadSum += predictedLoad.get(i+j);
                    effCapSum += effCap.get(i+j);
                }
                bw.write(actualLoadSum/j + "," + effCapSum/j);
                bw.newLine();
            }

            bw.close();
        } catch (IOException e) {
            record("Unable to write effective capacity");
            record(stackTraceToString(e));
            //System.exit(1);
        }
    }

    private void writeHourlyComparison2(ArrayList<Long> predictedLoad, ArrayList<Move> moves) {
        try {
            ArrayList<Long> effCap = new ArrayList<>();
            int t = 0;
            int nodes = 0;
            for (Move m : moves) {
                for (; t < m.time; ++t) {
                    effCap.add(Math.max(m.nodes, nodes) * MAX_CAPACITY_PER_SERVER);
                }
                nodes = m.nodes;
            }
            System.out.println("Size of moves: " + effCap.size());
            
            File f = new File(HOURLY_COMPARISON_FILE);
            FileWriter fw = new FileWriter(f);
            BufferedWriter bw = new BufferedWriter(fw);

            int window = 12;
            bw.write("actualLoad,effCap");
            bw.newLine();
            for (int i = 0; i < predictedLoad.size() && i < effCap.size(); i += window) {
                int actualLoadSum = 0, effCapSum = 0;
                int j = 0;
                for (; j < window && (i+j) < predictedLoad.size() && (i+j) < effCap.size(); ++j) {
                    actualLoadSum += predictedLoad.get(i+j);
                    effCapSum += effCap.get(i+j);
                }
                bw.write(actualLoadSum/j + "," + effCapSum/j);
                bw.newLine();
            }

            bw.close();
        } catch (IOException e) {
            record("Unable to write effective capacity");
            record(stackTraceToString(e));
            //System.exit(1);
        }
    }

    
    private void writeHourlyComparison(ArrayList<Long> actualLoad, ArrayList<Long> effCap) {
        try {
            File f = new File(HOURLY_COMPARISON_FILE);
            FileWriter fw = new FileWriter(f);
            BufferedWriter bw = new BufferedWriter(fw);

            int window = 360;
            bw.write("actualLoad,effCap");
            bw.newLine();
            for (int i = 0; i < actualLoad.size() && i < effCap.size(); i += window) {
                int actualLoadSum = 0, effCapSum = 0;
                int j = 0;
                for (; j < window && (i+j) < actualLoad.size() && (i+j) < effCap.size(); ++j) {
                    actualLoadSum += actualLoad.get(i+j);
                    effCapSum += effCap.get(i+j);
                }
                bw.write(actualLoadSum/j + "," + effCapSum/j);
                bw.newLine();
            }

            bw.close();
        } catch (IOException e) {
            record("Unable to write effective capacity");
            record(stackTraceToString(e));
            //System.exit(1);
        }
    }
    
    private int compareToActualLoad(String debug) {
        ArrayList<Long> effCap = getEffCap();
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
        ArrayList<Long> secondsAboveCapList = new ArrayList<>();
        for(int i = 0; i < actualLoad.size() && i < effCap.size(); ++i) {
            if (actualLoad.get(i) > effCap.get(i)) {
                secondsAboveCap++;
                secondsAboveCapList.add((long) i);
            }
        }
        
        //writeHourlyComparison(actualLoad, effCap);
        //System.out.println("Seconds above cap list: " + secondsAboveCapList.toString());
        double percentAboveCap = ((double) secondsAboveCap * 100)/(Math.min(actualLoad.size(), effCap.size()));
        double avgServers = ((double) getCost())/effCap.size();
        System.out.println("Actual load size: " + actualLoad.size());
        System.out.println("Effective capacity size: " + effCap.size());
        System.out.println("Seconds above capacity: " + secondsAboveCap);
        System.out.println("Percentage above capacity: " + percentAboveCap);
        System.out.println("Avg servers: " + avgServers);
        System.out.println(debug + "," + percentAboveCap + "," + avgServers);
        return secondsAboveCap;
    }
    
    private ArrayList<Long> testImpl(boolean useOraclePrediction, double predictionInflation, 
            double predictionPerturbation, String predictionFile, String config) {
        return testImpl(useOraclePrediction, false, predictionInflation, predictionPerturbation, predictionFile, 9, config);
    }
    
    private ArrayList<Long> testImpl(boolean useOraclePrediction, boolean reactiveOnly, double predictionInflation, 
            double predictionPerturbation, String predictionFile, int activeSites, String config) {
        record("Running the predictive controller simulation");

        TestPredictiveControllerSimulation c = new TestPredictiveControllerSimulation();
        try {
            c.runSimulation(useOraclePrediction, reactiveOnly, predictionInflation, predictionPerturbation, predictionFile, activeSites);
        } catch (Exception e) {
            e.printStackTrace();
            record("Not good");
            return null;
        }
        
        //System.out.println("Effective capacity: " + c.getEffCap());
        //c.writeEffCap();
        System.out.println("##########################################################");     
        System.out.println("Showing Results for:");
        System.out.println("    use oracle prediction: " + useOraclePrediction);
        System.out.println("    prediction inflation: " + predictionInflation);
        System.out.println("    prediction perturbation: " + predictionPerturbation);        
        System.out.println("    prediction file: " + predictionFile);
        System.out.println("    config: " + config);
        c.compareToActualLoad("PLOT: " + config + "," + predictionInflation);
        return c.getEffCap();
    }
    
    private ArrayList<Long> testStaticImpl(int totalMachines) {
        String config = "Static";
        System.out.println("##########################################################");     
        System.out.println("Showing Results for:");
        System.out.println("    use oracle prediction: " + false);
        System.out.println("    prediction inflation: " + totalMachines);
        System.out.println("    prediction perturbation: " + 0);        
        System.out.println("    prediction file: ");
        System.out.println("    config: " + config);

        ArrayList<Long> effCap = new ArrayList<>();
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
        
        for (int i = 0; i < actualLoad.size(); ++i) {
            effCap.add(totalMachines * MAX_CAPACITY_PER_SERVER_PER_SEC);
        }

        int secondsAboveCap = 0;       
        ArrayList<Long> secondsAboveCapList = new ArrayList<>();
        for(int i = 0; i < actualLoad.size() && i < effCap.size(); ++i) {
            if (actualLoad.get(i) > effCap.get(i)) {
                secondsAboveCap++;
                secondsAboveCapList.add((long) i);
            }
        }

        double percentAboveCap = ((double) secondsAboveCap * 100)/(Math.min(actualLoad.size(), effCap.size()));
        System.out.println("Actual load size: " + actualLoad.size());
        System.out.println("Effective capacity size: " + effCap.size());
        System.out.println("Seconds above capacity: " + secondsAboveCap);
        System.out.println("Percentage above capacity: " + percentAboveCap);
        System.out.println("Avg servers: " + totalMachines);
        System.out.println(config + "," + totalMachines + "," + percentAboveCap + "," + totalMachines);
        return effCap;
    }
    
    public void writeHourlyLoad(ArrayList<Long> load, BufferedWriter bw, String config) throws IOException {
        int window = 360;
        for (int i = 0; i < load.size(); i += window) {
            int loadSum = 0;
            int j = 0;
            for (; j < window && (i+j) < load.size(); ++j) {
                loadSum += load.get(i+j);
            }
            bw.write(config + "," + i/window + "," + loadSum/j);
            bw.newLine();
        }
    }
        
    public void testTimeSeries() throws Exception {
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
        
        try {
            File f = new File(HOURLY_COMPARISON_FILE);
            FileWriter fw = new FileWriter(f);
            BufferedWriter bw = new BufferedWriter(fw);

            bw.write("config,time,load");
            bw.newLine();

            writeHourlyLoad(actualLoad, bw, "Actual");
            writeHourlyLoad(testStaticImpl(8), bw, "Static");
            writeHourlyLoad(testImpl(false, true, 0.15, 0, "/data/rytaft/actual_load_5min_history.txt", 9, "Reactive"), bw, "Reactive");
            writeHourlyLoad(testImpl(true, false, 0.05, 0, "/data/rytaft/simple1.txt", 11, "Simple"), bw, "Simple");
            writeHourlyLoad(testImpl(false, 0.15, 0, "/data/rytaft/predpoints_forecastwindow_60_retrain1month_nullAsPrevLoad.txt", "P-Store SPAR"), bw, "P-Store SPAR");
            writeHourlyLoad(testImpl(false, 0.15, 0, "/data/rytaft/predpoints_forecastwindow_60_oracle.txt", "P-Store Oracle"), bw, "P-Store Oracle");
            
            bw.close();
        } catch (IOException e) {
            record("Unable to write effective capacity");
            record(stackTraceToString(e));
            //System.exit(1);
        }

        
    }

    public void testReactive() throws Exception {
        String simple1 = "/data/rytaft/actual_load_5min_history.txt";
        String config = "Reactive";
        testImpl(false, true, -0.35, 0, simple1, 9, config);
        testImpl(false, true, -0.25, 0, simple1, 9, config);
        testImpl(false, true, -0.20, 0, simple1, 9, config);
        testImpl(false, true, -0.15, 0, simple1, 9, config);
        testImpl(false, true, -0.10, 0, simple1, 9, config);
        testImpl(false, true, -0.05, 0, simple1, 9, config);
        testImpl(false, true, 0, 0, simple1, 10, config);
        testImpl(false, true, 0.05, 0, simple1, 10, config);
        testImpl(false, true, 0.10, 0, simple1, 11, config);
        testImpl(false, true, 0.15, 0, simple1, 11, config);
        testImpl(false, true, 0.20, 0, simple1, 12, config);
        testImpl(false, true, 0.25, 0, simple1, 12, config);
        testImpl(false, true, 0.35, 0, simple1, 13, config);
        testImpl(false, true, 0.50, 0, simple1, 15, config);
        testImpl(false, true, 0.50, 0, simple1, 15, config);
        testImpl(false, true, 1, 0, simple1, 19, config);
        testImpl(false, true, 2, 0, simple1, 29, config);
        testImpl(false, true, 3, 0, simple1, 39, config);
        testImpl(false, true, 5, 0, simple1, 59, config);
    }
    
//    public void testSimpleStrategy() throws Exception {
//        String simple1 = "/data/rytaft/simple1.txt";
//        String config = "Simple";
//        testImpl(true, -0.35, 0, simple1, config);
//        // testImpl(true, -0.25, 0, simple1, config);
//        // testImpl(true, -0.20, 0, simple1, config);
//        // testImpl(true, -0.15, 0, simple1, config);
//        // testImpl(true, -0.10, 0, simple1, config);
//        // testImpl(true, -0.05, 0, simple1, config);
//        // testImpl(true, 0, 0, simple1, 10, config);
//        // testImpl(true, 0.05, 0, simple1, 10, config);
//        // testImpl(true, 0.10, 0, simple1, 11, config);
//        // testImpl(true, 0.15, 0, simple1, 11, config);
//        // testImpl(true, 0.20, 0, simple1, 12, config);
//        // testImpl(true, 0.25, 0, simple1, 12, config);
//        // testImpl(true, 0.35, 0, simple1, 13, config);
//        // testImpl(true, 0.50, 0, simple1, 15, config);
//        // testImpl(true, 0.50, 0, simple1, 15, config);
//        // testImpl(true, 1, 0, simple1, 19, config);
//        // testImpl(true, 2, 0, simple1, 29, config);
//        // testImpl(true, 3, 0, simple1, 39, config);
//        // testImpl(true, 5, 0, simple1, 59, config);
//    }

    // public void testStatic() throws Exception {
    //     testStaticImpl(1);
    //     testStaticImpl(2);
    //     testStaticImpl(3);
    //     testStaticImpl(4);
    //     testStaticImpl(5);
    //     testStaticImpl(6);
    //     testStaticImpl(7);
    //     testStaticImpl(8);
    //     testStaticImpl(9);
    //     testStaticImpl(10);
    //     testStaticImpl(12);
    //     testStaticImpl(15);
    //     testStaticImpl(20);
    //     testStaticImpl(25);
    //     testStaticImpl(30);
    //     testStaticImpl(35);
    //     testStaticImpl(40);
    // }

    // public void testOptimal() throws Exception {
    //     String optimal = "/data/rytaft/actual_load_5min.txt";
    //     String config = "Optimal";
    //     testImpl(true, -0.25, 0, optimal, config);
    //     testImpl(true, -0.20, 0, optimal, config);
    //     testImpl(true, -0.15, 0, optimal, config);
    //     testImpl(true, -0.10, 0, optimal, config);
    //     testImpl(true, -0.05, 0, optimal, config);
    //     testImpl(true, 0, 0, optimal, 10, config);
    //     testImpl(true, 0.05, 0, optimal, 10, config);
    //     testImpl(true, 0.10, 0, optimal, 11, config);
    //     testImpl(true, 0.15, 0, optimal, 11, config);
    //     testImpl(true, 0.20, 0, optimal, 12, config);
    //     testImpl(true, 0.25, 0, optimal, 12, config);
    //     testImpl(true, 0.50, 0, optimal, 15, config);
    //     testImpl(true, 1, 0, optimal, 19, config);
    //     testImpl(true, 2, 0, optimal, 29, config);
    //     testImpl(true, 3, 0, optimal, 39, config);
    //     testImpl(true, 5, 0, optimal, 59, config);
    // }
    
//    public void testRealLoadSimulationTrainOnce() throws Exception {
//        String predTrainOnce = "/data/rytaft/predpoints_forecastwindow_60_train_once.txt";
//        testImpl(false, 0, 0, predTrainOnce, config);
//        testImpl(false, 0.05, 0, predTrainOnce, config);
//        testImpl(false, 0.10, 0, predTrainOnce, config);
//        testImpl(false, 0.15, 0, predTrainOnce, config);
//        testImpl(false, 0.20, 0, predTrainOnce, config);
//        testImpl(false, 0.25, 0, predTrainOnce, config);
//    }

    // public void testRealLoadSimulationRetrain1month() throws Exception {
    //     String predRetrain1month = "/data/rytaft/predpoints_forecastwindow_60_retrain1month_nullAsPrevLoad.txt";
    //     String config = "P-Store SPAR";
    //     // testImpl(false, -0.25, 0, predRetrain1month, config);
    //     // testImpl(false, -0.20, 0, predRetrain1month, config);
    //     // testImpl(false, -0.15, 0, predRetrain1month, config);
    //     // testImpl(false, -0.10, 0, predRetrain1month, config);
    //     // testImpl(false, -0.05, 0, predRetrain1month, config);
    //     // testImpl(false, 0, 0, predRetrain1month, config);
    //     // testImpl(false, 0.05, 0, predRetrain1month, config);
    //     // testImpl(false, 0.10, 0, predRetrain1month, config);
    //     // testImpl(false, 0.15, 0, predRetrain1month, config);
    //     // testImpl(false, 0.20, 0, predRetrain1month, config);
    //     // testImpl(false, 0.25, 0, predRetrain1month, config);
    //     // testImpl(false, 0.35, 0, predRetrain1month, config);
    //     // testImpl(false, 0.50, 0, predRetrain1month, 15, config);
    //     // testImpl(false, 1, 0, predRetrain1month, 19, config);
    //     // testImpl(false, 2, 0, predRetrain1month, 29, config);
    //     // testImpl(false, 3, 0, predRetrain1month, 39, config);
    //     // testImpl(false, 5, 0, predRetrain1month, 59, config);
    // }

    // public void testRealLoadSimulationOracle() throws Exception {
    //     String predOracle = "/data/rytaft/predpoints_forecastwindow_60_oracle.txt";
    //     String config = "P-Store Oracle";
    //     // testImpl(false, -0.25, 0, predOracle, config);
    //     // testImpl(false, -0.20, 0, predOracle, config);
    //     // testImpl(false, -0.15, 0, predOracle, config);
    //     // testImpl(false, -0.10, 0, predOracle, config);
    //     // testImpl(false, -0.05, 0, predOracle, config);
    //     // testImpl(false, 0, 0, predOracle, config);
    //     // testImpl(false, 0.05, 0, predOracle, config);
    //     // testImpl(false, 0.10, 0, predOracle, config);
    //     // testImpl(false, 0.15, 0, predOracle, config);
    //     // testImpl(false, 0.20, 0, predOracle, config);
    //     // testImpl(false, 0.25, 0, predOracle, config);
    //     // testImpl(false, 0.35, 0, predOracle, config);
    //     // testImpl(false, 0.50, 0, predOracle, 15, config);
    //     // testImpl(false, 1, 0, predOracle, 19, config);
    //     // testImpl(false, 2, 0, predOracle, 29, config);
    //     // testImpl(false, 3, 0, predOracle, 39, config);
    //     // testImpl(false, 5, 0, predOracle, 59, config);
    // }
        
//    public void testRealLoadSimulationOraclePerturbation() throws Exception {
//        String predOracle = "/data/rytaft/predpoints_forecastwindow_60_oracle.txt";
//        testImpl(false, 0, 0, predOracle, config);
//        testImpl(false, 0, 0.05, predOracle, config);
//        testImpl(false, 0, 0.10, predOracle, config);
//        testImpl(false, 0, 0.15, predOracle, config);
//        testImpl(false, 0, 0.20, predOracle, config);
//        testImpl(false, 0, 0.25, predOracle, config);
//    }
    

}
