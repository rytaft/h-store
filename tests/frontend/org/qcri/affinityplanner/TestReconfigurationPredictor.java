package org.qcri.affinityplanner;

import java.util.ArrayList;

import org.qcri.affinityplanner.ReconfigurationPredictor.Move;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

/**
 * @author rytaft
 */
public class TestReconfigurationPredictor extends BaseTestCase {

    private int partitions_per_site = 6;
    
    private long[] load_predictions_arr_1 = new long[]{
            100, 200, 300, 400, 500, 600, 600, 500, 400, 300, 200, 100, 
            100, 200, 300, 400, 500, 600, 600, 500, 400, 300, 200, 100 };
    private long capacity_per_node_1 = 100;
    private int nodes_start_1 = 1;
    private int db_migration_time_1 = 2;

    private long[] load_predictions_arr_2 = new long[]{
            600, 500, 400, 300, 200, 100, 100, 200, 300, 400, 500, 600, 
            600, 500, 400, 300, 200, 100, 100, 200, 300, 400, 500, 600 };
    private long capacity_per_node_2 = 100;
    private int nodes_start_2 = 6;
    private int db_migration_time_2 = 2;
    
    private long[] load_predictions_arr_3 = new long[]{
            100, 100, 100, 100, 100, 600, 100, 100, 100, 100, 100, 100, 
            100, 100, 100, 100, 100, 400, 100, 100, 100, 100, 100, 100 };
    private long capacity_per_node_3 = 100;
    private int nodes_start_3 = 1;
    private int db_migration_time_3 = 2;

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.B2W);
    }

    private void checkCorrect(ReconfigurationPredictor predictor, ArrayList<Move> moves,
                              long[] load_predictions_arr, boolean print) {
        Move prev_move = null;
        if (print) System.out.println("   Machines \tCapacity \tLoad");
        for (Move move : moves) {
            assertTrue(predictor.capacity(move.nodes) >= load_predictions_arr[move.time]);
            if (prev_move != null) {
                int reconfig_time = predictor.migration.reconfigTime(prev_move.nodes, move.nodes);
                for (int i = 1; i < move.time - prev_move.time; ++i) {
                    double effectiveCap = predictor.effectiveCapacity(i, reconfig_time, prev_move.nodes, move.nodes);
                    assertTrue(effectiveCap >= load_predictions_arr[prev_move.time+i]);
                    if (print) System.out.println(prev_move.time+i + ":   \t" + effectiveCap + " \t" + load_predictions_arr[prev_move.time+i]);
                }
            }
            if (print) System.out.println(move.time + ": " + move.nodes + " \t" + predictor.capacity(move.nodes) + " \t" + load_predictions_arr[move.time]);
            prev_move = move;

        }
    }
    
    private double totalCost(ReconfigurationPredictor predictor, ArrayList<Move> moves, boolean print, String debug) {
        double cost = 0;
        Move last_move = null;
        for (Move move : moves) {
            if (last_move != null) {
                cost += predictor.migration.moveCost(move.time - last_move.time, last_move.nodes, move.nodes);
            }
            
            last_move = move;
        }
        
        if (print) System.out.println(debug + "Total cost: " + cost);
        return cost;
    }

    public void testBestMoves1() throws Exception {
        testBestMoves1(new SingleThreadedMigration(db_migration_time_1), "Single Threaded Migration: ");
        testBestMoves1(new ParallelMigration(partitions_per_site, db_migration_time_1), "Parallel Migration: ");
        testBestMoves1(new ParallelMigration(partitions_per_site, db_migration_time_1 * 10), "Parallel Migration: ");
    }
       
    private void testBestMoves1(Migration migration, String debug) throws Exception {
        ArrayList<Long> load_predictions = new ArrayList<>();
        for (long prediction : load_predictions_arr_1) load_predictions.add(prediction);

        // defaults
        ReconfigurationPredictor predictor = new ReconfigurationPredictor(capacity_per_node_1, migration);
        ArrayList<Move> moves = predictor.bestMoves(load_predictions, nodes_start_1);
        totalCost(predictor, moves, true, debug);
        checkCorrect(predictor, moves, load_predictions_arr_1, true);

        // increased capacity
        predictor = new ReconfigurationPredictor(capacity_per_node_1 + 10, migration);
        moves = predictor.bestMoves(load_predictions, nodes_start_1);
        totalCost(predictor, moves, true, debug);
        checkCorrect(predictor, moves, load_predictions_arr_1, true);

        // increased starting partitions
        predictor = new ReconfigurationPredictor(capacity_per_node_1, migration);
        moves = predictor.bestMoves(load_predictions, nodes_start_1 + 1);
        totalCost(predictor, moves, true, debug);
        checkCorrect(predictor, moves, load_predictions_arr_1, true);

        // increased time to move data
        migration.setDbMigrationTime(3);
        predictor = new ReconfigurationPredictor(capacity_per_node_1, migration);
        moves = predictor.bestMoves(load_predictions, nodes_start_1);
        if (migration instanceof SingleThreadedMigration) {
            assertTrue(moves == null);
        }
    }

    public void testBestMoves2() throws Exception {
        testBestMoves2(new SingleThreadedMigration(db_migration_time_2), "Single Threaded Migration: ");
        testBestMoves2(new ParallelMigration(partitions_per_site, db_migration_time_2), "Parallel Migration: ");
        testBestMoves2(new ParallelMigration(partitions_per_site, db_migration_time_2 * 10), "Parallel Migration: ");
    }
        
    private void testBestMoves2(Migration migration, String debug) throws Exception {
        ArrayList<Long> load_predictions = new ArrayList<>();
        for (long prediction : load_predictions_arr_2) load_predictions.add(prediction);

        // defaults
        ReconfigurationPredictor predictor = new ReconfigurationPredictor(capacity_per_node_2, migration);
        ArrayList<Move> moves = predictor.bestMoves(load_predictions, nodes_start_2);
        totalCost(predictor, moves, true, debug);
        checkCorrect(predictor, moves, load_predictions_arr_2, true);

        // increased capacity
        predictor = new ReconfigurationPredictor(capacity_per_node_2 + 10, migration);
        moves = predictor.bestMoves(load_predictions, nodes_start_2);
        totalCost(predictor, moves, true, debug);
        checkCorrect(predictor, moves, load_predictions_arr_2, true);

        // increased starting partitions
        predictor = new ReconfigurationPredictor(capacity_per_node_2, migration);
        moves = predictor.bestMoves(load_predictions, nodes_start_2 + 1);
        totalCost(predictor, moves, true, debug);
        checkCorrect(predictor, moves, load_predictions_arr_2, true);

        // increased time to move data
        migration.setDbMigrationTime(3);
        predictor = new ReconfigurationPredictor(capacity_per_node_2, migration);
        moves = predictor.bestMoves(load_predictions, nodes_start_2);
        totalCost(predictor, moves, true, debug);
        checkCorrect(predictor, moves, load_predictions_arr_2, true);
    }
    
    public void testBestMoves3() throws Exception {
        testBestMoves3(new SingleThreadedMigration(db_migration_time_3), "Single Threaded Migration: ");
        testBestMoves3(new ParallelMigration(partitions_per_site, db_migration_time_3), "Parallel Migration: ");
        testBestMoves3(new ParallelMigration(partitions_per_site, db_migration_time_3 * 10), "Parallel Migration: ");
    }
    
    private void testBestMoves3(Migration migration, String debug) throws Exception {
        ArrayList<Long> load_predictions = new ArrayList<>();
        for (long prediction : load_predictions_arr_3) load_predictions.add(prediction);

        // defaults
        ReconfigurationPredictor predictor = new ReconfigurationPredictor(capacity_per_node_3, migration);
        ArrayList<Move> moves = predictor.bestMoves(load_predictions, nodes_start_3);
        totalCost(predictor, moves, true, debug);
        checkCorrect(predictor, moves, load_predictions_arr_3, true);

        // increased capacity
        predictor = new ReconfigurationPredictor(capacity_per_node_3 + 10, migration);
        moves = predictor.bestMoves(load_predictions, nodes_start_3);
        totalCost(predictor, moves, true, debug);
        checkCorrect(predictor, moves, load_predictions_arr_3, true);

        // increased starting partitions
        predictor = new ReconfigurationPredictor(capacity_per_node_3, migration);
        moves = predictor.bestMoves(load_predictions, nodes_start_3 + 1);
        totalCost(predictor, moves, true, debug);
        checkCorrect(predictor, moves, load_predictions_arr_3, true);

        // increased time to move data
        migration.setDbMigrationTime(6);
        predictor = new ReconfigurationPredictor(capacity_per_node_3, migration);
        moves = predictor.bestMoves(load_predictions, nodes_start_3);
        totalCost(predictor, moves, true, debug);
        checkCorrect(predictor, moves, load_predictions_arr_3, true);
        
        // increased time to move data
        migration.setDbMigrationTime(7);
        predictor = new ReconfigurationPredictor(capacity_per_node_3, migration);
        moves = predictor.bestMoves(load_predictions, nodes_start_3);
        if (migration instanceof SingleThreadedMigration) {
            assertTrue(moves == null);
        }
        else {
            checkCorrect(predictor, moves, load_predictions_arr_3, true);
        }
    }

    public void testBestMoves4() throws Exception {
        testBestMoves4(new SingleThreadedMigration(500), "Single Threaded Migration: ");
        testBestMoves4(new ParallelMigration(partitions_per_site, 500), "Parallel Migration: ");
        testBestMoves4(new ParallelMigration(partitions_per_site, 5000), "Parallel Migration: ");
    }
    
    private void testBestMoves4(Migration migration, String debug) throws Exception {
        ArrayList<Long> load_predictions = new ArrayList<>();
        long load = 100;
        // first hump
        for (int i = 0; i < 2000; ++i) {
            load_predictions.add(load);
            load += 50;
        }
        for (int i = 0; i < 2000; ++i) {
            load_predictions.add(load);
            load += 10;
        }
        for (int i = 0; i < 4000; ++i) {
            load_predictions.add(load);
        }
        for (int i = 0; i < 2000; ++i) {
            load_predictions.add(load);
            load -= 10;
        }
        for (int i = 0; i < 2000; ++i) {
            load_predictions.add(load);
            load -= 50;
        }

        // second hump
        for (int i = 0; i < 2000; ++i) {
            load_predictions.add(load);
            load += 50;
        }
        for (int i = 0; i < 2000; ++i) {
            load_predictions.add(load);
            load += 10;
        }
        for (int i = 0; i < 4000; ++i) {
            load_predictions.add(load);
        }
        for (int i = 0; i < 2000; ++i) {
            load_predictions.add(load);
            load -= 10;
        }
        for (int i = 0; i < 2000; ++i) {
            load_predictions.add(load);
            load -= 50;
        }

        // spike
        for (int i = 0; i < 2000; ++i) {
            load_predictions.add(load);
            load += 200;
        }
        for (int i = 0; i < 2000; ++i) {
            load_predictions.add(load);
            load -= 200;
        }
        for (int i = 0; i < 2000; ++i) {
            load_predictions.add(load);
            load += 200;
        }

        ReconfigurationPredictor predictor = new ReconfigurationPredictor(8000, migration);

        ArrayList<Move> moves = predictor.bestMoves(load_predictions, 2);
        long load_predictions_arr[] = new long[load_predictions.size()];
        for (int i = 0; i < load_predictions_arr.length; ++i) load_predictions_arr[i] = load_predictions.get(i);
        totalCost(predictor, moves, true, debug);
        checkCorrect(predictor, moves, load_predictions_arr, false);

        System.out.println("Max machines: " + predictor.getMaxNodes() + ", Time steps: " + load_predictions_arr.length);
    }

}
