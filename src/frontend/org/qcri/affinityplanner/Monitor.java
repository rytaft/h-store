/**
 * @author Marco
 */

package org.qcri.affinityplanner;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
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
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hashing.ExplicitHasher;
import edu.brown.hashing.ExplicitPartitions;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.PartitionEstimator;

public class Monitor {
    private static final Logger LOG = Logger.getLogger(ExplicitPartitions.class);

    private final CatalogContext m_catalog_context;
    private final PartitionEstimator m_p_estimator;
    private int max_entries;
    int m_curr_entries = 0;

    private BufferedWriter m_writer;
    private Path m_logFile;
    private Path m_intervalPath;
    private long m_start_monitoring;
    private boolean m_is_monitoring;
    
    public Monitor(CatalogContext catalog_context, PartitionEstimator p_estimator, int partitionId){
        m_catalog_context = catalog_context;
        m_p_estimator = p_estimator;
        max_entries = Integer.MAX_VALUE;
    }

    public Monitor(CatalogContext catalog_context, PartitionEstimator p_estimator, int partitionId, int maxEntries){
        this(catalog_context, p_estimator, partitionId);
        max_entries = maxEntries;
    }
    
    /**
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
     *  If VERBOSE = false, it outputs a CSV of the form
     * 
     *  TRANSACTION_ID, TABLE_NAME, COLUMN_NAME, VAL
     *  
     *  with one entry per SQL statement. There can be multiple equal entries if a tuple is accessed multiple times
     *  
     *  Returns true if it has not already logged a max number of entries, false otherwise
     */
    public boolean logPartitioningAttributes (LocalTransaction ts, long[] fragmentIds, ParameterSet[] parameterSets){

        String s = null;
        m_curr_entries ++;
        if (m_curr_entries > max_entries){
            LOG.warn("Must close access log file because of too many entries");
            closeLog();
            return false;
        }
        
        for (int i = 0; i < fragmentIds.length; i++) {
            List<Pair<List<CatalogType>, List<Integer>>> offsets = new ArrayList<>();
            // the following is slow but we can just keep a map to speed it up if needed - similar to columnToTable
            m_p_estimator.getPlanFragmentEstimationParametersMultiCol(CatalogUtil.getPlanFragment(m_catalog_context.database, (int) fragmentIds[i]), offsets);
            ParameterSet parameterSet = parameterSets[i];
            for (Pair<List<CatalogType>, List<Integer>> offsetPair : offsets) {
                // considering a specific Table here
                try{
                	String table_name = ((ExplicitHasher) this.m_p_estimator.getHasher()).getPartitions().getParentTableName((Column) offsetPair.getFirst().get(0));
                	
                    if (table_name == null){
                        LOG.warn("Monitoring cannot determine the table accessed by a transaction");
                    }
                    else{
                        Iterator<CatalogType> columnIter = offsetPair.getFirst().iterator();
                        for(Integer offset : offsetPair.getSecond()) {
                            Column column = (Column) columnIter.next();
                            s = ts.getTransactionId().toString() + ";" + table_name + "," + column.getName().toLowerCase() + "," + parameterSet.toArray()[offset];
                            m_writer.write(s, 0, s.length());
                            m_writer.newLine();
                        }
                    }
                } catch (ClassCastException e) {
                    e.printStackTrace();
                   LOG.warn("Monitoring cannot determine the table accessed by a transaction");
                } catch (IOException e) {
                    e.printStackTrace();
                    LOG.warn("Failed while writing file " + m_logFile.toString());
                }
            }
        }
        return true;
    }
    
    public void logFinishTransaction(Long trans_id){
        String s = "END;" + trans_id.toString();
        try {
            m_writer.write(s, 0, s.length());
            m_writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
            LOG.warn("Failed while writing file " + m_logFile.toString());
        }
    }
    
    public void openLog(Path logFile, Path intervalPath){
        if(m_is_monitoring){
            LOG.warn("Monitor opened accessed monitoring log - " + logFile.toString() + " - before closing the previous one - " + m_logFile.toString());
            closeLog();
        }
        m_start_monitoring = System.currentTimeMillis();
        m_intervalPath = intervalPath;
        m_logFile = logFile;
        try {
            m_writer = Files.newBufferedWriter(m_logFile, Charset.forName("US-ASCII"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            m_writer = null;
            LOG.warn("Monitor failed while creating file " + m_logFile.toString());
            return;
       }       
        m_is_monitoring = true;
    }
    
    public void closeLog(){
        if(! m_is_monitoring){
            LOG.warn("Monitor tried to close access log but it was not open");
            System.out.println("Monitor tried to close access log but it was not open");
        }
        else{
            try {
                m_writer.close();
            } catch (IOException e) {
                LOG.warn("Monitor failed while closing file " + m_logFile.toString() + "\nStack trace " + Controller.stackTraceToString(e));
            }
            try {
                long interval = System.currentTimeMillis() - m_start_monitoring;
                BufferedWriter intervalFile;
                intervalFile = Files.newBufferedWriter(m_intervalPath, Charset.forName("US-ASCII"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                intervalFile.write(Long.toString(interval));
                intervalFile.close();
            } catch (IOException e) {
                LOG.warn("Monitor failed while writing time interval file " + m_intervalPath.toString() + ".\nStack trace " + Controller.stackTraceToString(e));
            }
        }
        m_is_monitoring = false;
    }
    
    public boolean isMonitoring(){
        return m_is_monitoring;
    }
}
