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
    private BufferedWriter m_writer;
    private Path m_logFile;
    private ColumnToTableMap m_columnToTable;
    
    private CatalogContext m_catalog_context;
    private PartitionEstimator m_p_estimator;

    final int MAX_ENTRIES = Integer.MAX_VALUE;
    int m_curr_entries = 0;
    
    final boolean VERBOSE = false;

    public Monitor(CatalogContext catalog_context, PartitionEstimator p_estimator, int partitionId){
        this.m_columnToTable = new ColumnToTableMap(catalog_context);
        this.m_catalog_context = catalog_context;
        this.m_p_estimator = p_estimator;
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
     *  TRANSACTION_ID, TABLE_NAME, COLUMN_NAME, VAL
     *  
     *  With one entry per SQL statement. There can be multiple equal entries if a tuple is accessed multiple times
     *  
     *  Returns true if it has not already logged a max number of entries, false otherwise
     */
    public boolean logPartitioningAttributes (LocalTransaction ts, long[] fragmentIds, ParameterSet[] parameterSets){

        String s = null;
        this.m_curr_entries ++;
        if (m_curr_entries > MAX_ENTRIES){
            LOG.warn("Must close access log file because of too many entries");
            try {
                this.m_writer.close();
            } catch (IOException e) {
                e.printStackTrace();
                LOG.warn("Failed while closing file " + this.m_logFile.toString());
            }
            return false;
        }

        // DEBUG begin
        if(this.VERBOSE){
            try {
                m_writer.newLine();
                m_writer.newLine();
                s = "New transaction with id " + ts.getTransactionId();
                this.m_writer.write(s, 0, s.length());
               
                m_writer.newLine();
                s = ts.getProcedure().getName();
                this.m_writer.write(s, 0, s.length());
    
//                s = "\nPARAMS";
//                this.writer.write(s, 0, s.length());
//                for (ParameterSet paramSet : parameterSets){
//                    s = "\n"+paramSet.toString();
//                    writer.write(s, 0, s.length());
//                }
                
                m_writer.newLine();
                s = "TUPLES";
                this.m_writer.write(s, 0, s.length());
            } catch (IOException e) {
                e.printStackTrace();
                LOG.warn("Failed while writing file " + this.m_logFile.toString());
            }
        }
        // DEBUG end
        
        for (int i = 0; i < fragmentIds.length; i++) {
            List<Pair<List<CatalogType>, List<Integer>>> offsets = new ArrayList<>();
            // the following is slow but we can just keep a map to speed it up if needed - similar to columnToTable
            m_p_estimator.getPlanFragmentEstimationParametersMultiCol(CatalogUtil.getPlanFragment(this.m_catalog_context.database, (int) fragmentIds[i]), offsets);
            ParameterSet parameterSet = parameterSets[i];
            for (Pair<List<CatalogType>, List<Integer>> offsetPair : offsets) {
                // considering a specific Table here
                Table table = null;
                try{
                    table = this.m_columnToTable.getTable((Column) offsetPair.getFirst().get(0));
                    if (table == null){
                        LOG.warn("Monitoring cannot determine the table accessed by a transaction");
                    }
                    else{
                        if(this.VERBOSE){
                            m_writer.newLine();
                            s ="Table:" + table.getName() + " -- ";
                            this.m_writer.write(s, 0, s.length());
                        }
                        Iterator<CatalogType> columnIter = offsetPair.getFirst().iterator(); // for debugging
                        for(Integer offset : offsetPair.getSecond()) {
                            Column column = (Column) columnIter.next();
                            if(this.VERBOSE){
                                s = "Coulumn: " + column.getName() + " Val: " + parameterSet.toArray()[offset] + " -- ";
                            }
                            else{
                                s = ts.getTransactionId().toString() + ";" + table.getName() + "," + column.getName() + "," + parameterSet.toArray()[offset];
                            }
                            this.m_writer.write(s, 0, s.length());
                            m_writer.newLine();
                        }
                    }
                } catch (ClassCastException e) {
                    e.printStackTrace();
                   LOG.warn("Monitoring cannot determine the table accessed by a transaction");
                } catch (IOException e) {
                    e.printStackTrace();
                    LOG.warn("Failed while writing file " + this.m_logFile.toString());
                }
            }
        }
        return true;
    }
    
    public void openLog(Path logFile){
        if(m_writer != null){
            LOG.warn("opened accessed monitoring log - " + logFile.toString() + " - before closing the previous one - " + this.m_logFile.toString());
        }
        this.m_logFile = logFile;
        try {
            this.m_writer = Files.newBufferedWriter(m_logFile, Charset.forName("US-ASCII"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            this.m_writer = null;
            LOG.warn("Failed while creating file " + this.m_logFile.toString());
            System.out.println("Failed while creating file " + this.m_logFile.toString());
       }       
    }
    
    public void closeLog(){
        if(m_writer == null){
            LOG.warn("tried to close access log but it was not open");
        }
        else{
            try {
                this.m_writer.close();
            } catch (IOException e) {
                e.printStackTrace();
                LOG.warn("Failed while closing file " + this.m_logFile.toString());
            }            
        }
    }
}
