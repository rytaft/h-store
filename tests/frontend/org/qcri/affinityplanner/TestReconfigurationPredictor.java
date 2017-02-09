package org.qcri.affinityplanner;

import java.util.ArrayList;

import org.qcri.affinityplanner.ReconfigurationPredictor.Move;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

/**
 * @author rytaft
 */
public class TestReconfigurationPredictor extends BaseTestCase {
    
    private double[] load_predictions_arr = new double[]{ 
            100, 200, 300, 400, 500, 600, 600, 500, 400, 300, 200, 100, 
            100, 200, 300, 400, 500, 600, 600, 500, 400, 300, 200, 100 };
    private double capacity_per_node = 101;
    private int nodes_start = 1;
    private int db_migration_time = 2;
    ReconfigurationPredictor predictor;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.B2W);
        ArrayList<Double> load_predictions = new ArrayList<>();
        for (double prediction : load_predictions_arr) load_predictions.add(prediction);
        predictor = new ReconfigurationPredictor(capacity_per_node, 
                load_predictions, nodes_start, db_migration_time);
    }
    
    public void testBestMoves() throws Exception {
        ArrayList<Move> moves = predictor.bestMoves();
        Move prev_move = null;
        for (Move move : moves) {
            assertTrue(predictor.capacity(move.nodes) >= load_predictions_arr[move.time]);
            if (prev_move != null) {
                int reconfig_time = predictor.reconfigTime(prev_move.nodes, move.nodes);
                for (int i = 1; i < move.time - prev_move.time; ++i) {
                    double effectiveCap = predictor.effectiveCapacity(i, reconfig_time, prev_move.nodes, move.nodes);
                    assertTrue(effectiveCap >= load_predictions_arr[prev_move.time+i]);
                }
            }
            prev_move = move;
        }
    }
    
}
