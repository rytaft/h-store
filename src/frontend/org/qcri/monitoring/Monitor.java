/*
 * @author Marco
 */

package org.qcri.monitoring;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.ParameterSet;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hashing.ExplicitPartitions;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.PartitionEstimator;

public class Monitor {
    private static final Logger LOG = Logger.getLogger(ExplicitPartitions.class);
    private BufferedWriter writer;
    private Path logFile;
    private  boolean monitoring = true;
    private ColumnToTableMap columnToTable;
    
    private CatalogContext catalog_context;
    private PartitionEstimator p_estimator;

    final int MAX_ENTRIES = 100;
    int curr_entries = 0;
    
    final boolean VERBOSE = false;

    public Monitor(CatalogContext catalog_context, PartitionEstimator p_estimator, int partitionId){
        this.columnToTable = new ColumnToTableMap(catalog_context);
        this.catalog_context = catalog_context;
        this.p_estimator = p_estimator;
        // TODO one file per partition executor to avoid concurrent IO. will have to be merged at site level for complete stats.
        logFile = FileSystems.getDefault().getPath(".", "transactions-partition-" + partitionId + ".log");
        try {
            this.writer = Files.newBufferedWriter(logFile, Charset.forName("US-ASCII"), StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            LOG.warn("Failed while creating file " + this.logFile.toString());
            System.out.println("Failed while creating file " + this.logFile.toString());
       }
    }
    
    /*
     * This method is called every time a batch of SQL statements is invoked. A transaction consists of multiple SQL statements. 
     * Typically, SQL statement batches are called like this in bechmarks:
     * 
     *  voltQueueSQL(stmt, params);
     *  voltQueueSQL(stmt, params);
     *  ...
     *  voltQueueSQL(stmt, params);
     *  voltExecuteSQL(true);
     *  
     *  A pattern like this is a batch. There can be multiple batches in a transaction.
     *  
     *  If VERBOSE = false, it output a CSV of the form
     * 
     *  TRAN_ID, TABLE_NAME, COLUMN_NAME, VAL
     *  
     *  There can be multiple equal entries if a tuple is accessed multiple times
     */
    public void logPartitioningAttributes (LocalTransaction ts, long[] fragmentIds, ParameterSet[] parameterSets){
        if (!monitoring) return;

        String s = null;
        this.curr_entries ++;
        if (curr_entries > MAX_ENTRIES){
            try {
                this.writer.close();
            } catch (IOException e) {
                e.printStackTrace();
                LOG.warn("Failed while closing file " + this.logFile.toString());
            }
            monitoring = false;
            return;
        }

        if(this.VERBOSE){
            try {
                writer.newLine();
                writer.newLine();
                s = "New transaction with id " + ts.getTransactionId();
                this.writer.write(s, 0, s.length());
               
                writer.newLine();
                s = ts.getProcedure().getName();
                this.writer.write(s, 0, s.length());
    
//                s = "\nPARAMS";
//                this.writer.write(s, 0, s.length());
//                for (ParameterSet paramSet : parameterSets){
//                    s = "\n"+paramSet.toString();
//                    writer.write(s, 0, s.length());
//                }
                
                writer.newLine();
                s = "TUPLES";
                this.writer.write(s, 0, s.length());
            } catch (IOException e) {
                e.printStackTrace();
                LOG.warn("Failed while writing file " + this.logFile.toString());
            }
        }
        for (int i = 0; i < fragmentIds.length; i++) {
            List<Pair<List<CatalogType>, List<Integer>>> offsets = new ArrayList<>();
            // the following is slow but we can just keep a map to speed it up if needed - similar to columnToTable
            p_estimator.getPlanFragmentEstimationParametersMultiCol(CatalogUtil.getPlanFragment(this.catalog_context.database, (int) fragmentIds[i]), offsets);
            ParameterSet parameterSet = parameterSets[i];
            for (Pair<List<CatalogType>, List<Integer>> offsetPair : offsets) {
                // considering a specific Table here
                Table table = null;
                try{
                    table = this.columnToTable.getTable((Column) offsetPair.getFirst().get(0));
                    if (table == null){
                        LOG.warn("Monitoring cannot determine the table accessed by a transaction");
                    }
                    else{
                        if(this.VERBOSE){
                            writer.newLine();
                            s ="Table:" + table.getName() + " -- ";
                            this.writer.write(s, 0, s.length());
                        }
                        Iterator<CatalogType> columnIter = offsetPair.getFirst().iterator(); // for debugging
                        for(Integer offset : offsetPair.getSecond()) {
                            Column column = (Column) columnIter.next();
                            if(this.VERBOSE){
                                s = "Coulumn: " + column.getName() + " Val: " + parameterSet.toArray()[offset] + " -- ";
                            }
                            else{
                                s = ts.getTransactionId().toString() + "," + table.getName() + "," + column.getName() + "," + parameterSet.toArray()[offset];
                            }
                            this.writer.write(s, 0, s.length());
                            writer.newLine();
                        }
                    }
                } catch (ClassCastException e) {
                    e.printStackTrace();
                   LOG.warn("Monitoring cannot determine the table accessed by a transaction");
                } catch (IOException e) {
                    e.printStackTrace();
                    LOG.warn("Failed while writing file " + this.logFile.toString());
                }
            }
        }
        // 
    }
}
