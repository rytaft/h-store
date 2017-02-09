package org.qcri.affinityplanner;

import java.util.ArrayList;

import org.qcri.affinityplanner.ReconfigurationPredictor.Move;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

/**
 * @author rytaft
 */
public class TestReconfigurationPredictor extends BaseTestCase {
    
    private double[] load_predictions_arr_1 = new double[]{ 
            100, 200, 300, 400, 500, 600, 600, 500, 400, 300, 200, 100, 
            100, 200, 300, 400, 500, 600, 600, 500, 400, 300, 200, 100 };
    private double capacity_per_node_1 = 100;
    private int nodes_start_1 = 1;
    private int db_migration_time_1 = 2;
    
    private double[] load_predictions_arr_2 = new double[]{ 
            600, 500, 400, 300, 200, 100, 100, 200, 300, 400, 500, 600, 
            600, 500, 400, 300, 200, 100, 100, 200, 300, 400, 500, 600 };
    private double capacity_per_node_2 = 100;
    private int nodes_start_2 = 6;
    private int db_migration_time_2 = 2;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.B2W);
    }
    
    private void checkCorrect(ReconfigurationPredictor predictor, ArrayList<Move> moves, 
            double[] load_predictions_arr, boolean print) {
        Move prev_move = null;
        if (print) System.out.println("   Capacity \tLoad");
        for (Move move : moves) {
            assertTrue(predictor.capacity(move.nodes) >= load_predictions_arr[move.time]);
            if (prev_move != null) {
                int reconfig_time = predictor.reconfigTime(prev_move.nodes, move.nodes);
                for (int i = 1; i < move.time - prev_move.time; ++i) {
                    double effectiveCap = predictor.effectiveCapacity(i, reconfig_time, prev_move.nodes, move.nodes);
                    assertTrue(effectiveCap >= load_predictions_arr[prev_move.time+i]);
                    if (print) System.out.println(prev_move.time+i + ": " + effectiveCap + " \t" + load_predictions_arr[prev_move.time+i]);
                }
            }
            if (print) System.out.println(move.time + ": " + predictor.capacity(move.nodes) + " \t" + load_predictions_arr[move.time]);
            prev_move = move;
            
        }
    }
    
    public void testBestMoves1() throws Exception {
        ArrayList<Double> load_predictions = new ArrayList<>();
        for (double prediction : load_predictions_arr_1) load_predictions.add(prediction);

        // defaults
        ReconfigurationPredictor predictor = new ReconfigurationPredictor(capacity_per_node_1, 
                load_predictions, nodes_start_1, db_migration_time_1);        
        ArrayList<Move> moves = predictor.bestMoves();
        checkCorrect(predictor, moves, load_predictions_arr_1, true);
        
        // increased capacity
        predictor = new ReconfigurationPredictor(capacity_per_node_1 + 10, 
                load_predictions, nodes_start_1, db_migration_time_1);
        moves = predictor.bestMoves();
        checkCorrect(predictor, moves, load_predictions_arr_1, true);
        
        // increased starting nodes
        predictor = new ReconfigurationPredictor(capacity_per_node_1, 
                load_predictions, nodes_start_1 + 1, db_migration_time_1);
        moves = predictor.bestMoves();
        checkCorrect(predictor, moves, load_predictions_arr_1, true);
        
        // increased time to move data
        predictor = new ReconfigurationPredictor(capacity_per_node_1, 
                load_predictions, nodes_start_1, 3);
        moves = predictor.bestMoves();
        assertTrue(moves == null);
    }
    
    public void testBestMoves2() throws Exception {
        ArrayList<Double> load_predictions = new ArrayList<>();
        for (double prediction : load_predictions_arr_2) load_predictions.add(prediction);

        // defaults
        ReconfigurationPredictor predictor = new ReconfigurationPredictor(capacity_per_node_2, 
                load_predictions, nodes_start_2, db_migration_time_2);        
        ArrayList<Move> moves = predictor.bestMoves();
        checkCorrect(predictor, moves, load_predictions_arr_2, true);
        
        // increased capacity
        predictor = new ReconfigurationPredictor(capacity_per_node_2 + 10, 
                load_predictions, nodes_start_2, db_migration_time_2);
        moves = predictor.bestMoves();
        checkCorrect(predictor, moves, load_predictions_arr_2, true);
        
        // increased starting nodes
        predictor = new ReconfigurationPredictor(capacity_per_node_2, 
                load_predictions, nodes_start_2 + 1, db_migration_time_2);
        moves = predictor.bestMoves();
        checkCorrect(predictor, moves, load_predictions_arr_2, true);
        
        // increased time to move data
        predictor = new ReconfigurationPredictor(capacity_per_node_2, 
                load_predictions, nodes_start_2, 3);
        moves = predictor.bestMoves();
        checkCorrect(predictor, moves, load_predictions_arr_2, true);
    }
    
    public void testBestMoves3() throws Exception {
        ArrayList<Double> load_predictions = new ArrayList<>();
        double load = 100;
        // first hump
        for (int i = 0; i < 500; ++i) {
            load_predictions.add(load);
            load += 50;
        }
        for (int i = 0; i < 500; ++i) {
            load_predictions.add(load);
            load += 10;
        }
        for (int i = 0; i < 1000; ++i) {
            load_predictions.add(load);
        }
        for (int i = 0; i < 500; ++i) {
            load_predictions.add(load);
            load -= 10;
        }
        for (int i = 0; i < 500; ++i) {
            load_predictions.add(load);
            load -= 50;
        }
        
        // second hump
        for (int i = 0; i < 500; ++i) {
            load_predictions.add(load);
            load += 50;
        }
        for (int i = 0; i < 500; ++i) {
            load_predictions.add(load);
            load += 10;
        }
        for (int i = 0; i < 1000; ++i) {
            load_predictions.add(load);
        }
        for (int i = 0; i < 500; ++i) {
            load_predictions.add(load);
            load -= 10;
        }
        for (int i = 0; i < 500; ++i) {
            load_predictions.add(load);
            load -= 50;
        }
        
        // spike
        for (int i = 0; i < 500; ++i) {
            load_predictions.add(load);
            load += 200;
        }
        for (int i = 0; i < 500; ++i) {
            load_predictions.add(load);
            load -= 200;
        }
        
        ReconfigurationPredictor predictor = new ReconfigurationPredictor(200, load_predictions, 2, 10);
        
        ArrayList<Move> moves = predictor.bestMoves();
        double load_predictions_arr[] = new double[load_predictions.size()];
        for (int i = 0; i < load_predictions_arr.length; ++i) load_predictions_arr[i] = load_predictions.get(i);
        checkCorrect(predictor, moves, load_predictions_arr, false);
        
        System.out.println("Max machines: " + predictor.getMaxNodes() + ", Time steps: " + load_predictions_arr.length);
    }
    
}
