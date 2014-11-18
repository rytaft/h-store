package edu.mit.benchmark.affinity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;

import edu.brown.api.BenchmarkComponent;
import edu.brown.api.Loader;
import edu.brown.benchmark.ycsb.YCSBUtil;
import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ThreadUtil;

public class AffinityLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(AffinityLoader.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    private final long init_record_count;
    private int loadthreads = ThreadUtil.availableProcessors();

    public static void main(String args[]) throws Exception {
        if (debug.val)
            LOG.debug("MAIN: " + AffinityLoader.class.getName());
        BenchmarkComponent.main(AffinityLoader.class, args, true);
    }
    
    public AffinityLoader(String[] args) {
        super(args);
        if (debug.val)
            LOG.debug("CONSTRUCTOR: " + AffinityLoader.class.getName());
        
        boolean useFixedSize = false;
        long fixedSize = -1;
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            // Used Fixed-size Database
            // Parameter that points to where we can find the initial data files
            if  (key.equalsIgnoreCase("num_records")) {
                fixedSize = Long.valueOf(value);
            }
            // Multi-Threaded Loader
            else if (key.equalsIgnoreCase("loadthreads")) {
                this.loadthreads = Integer.valueOf(value);
            }
        } // FOR
        
        // Figure out the # of records that we need
        if (useFixedSize && fixedSize > 0) {
            this.init_record_count = fixedSize;
        }
        else {
            this.init_record_count = (int)Math.round(AffinityConstants.NUM_RECORDS * 
                                                     this.getScaleFactor());
        }
        LOG.info("Initializing database with " + init_record_count + " records. Using " + this.loadthreads + " load threads");
    }

    @Override
    public void load() throws IOException {
        if (debug.val)
            LOG.debug("Starting AffinityLoader");

        final CatalogContext catalogContext = this.getCatalogContext(); 
        final Table catalog_tbl_a = catalogContext.getTableByName(AffinityConstants.TABLE_A_NAME);
        final AtomicLong total = new AtomicLong(0);
        
        // Multi-threaded loader
        final int rows_per_thread = (int)Math.ceil(init_record_count / (double)this.loadthreads);
        final List<Runnable> runnables = new ArrayList<Runnable>();
        for (int i = 0; i < this.loadthreads; i++) {
            final int thread_id = i;
            final int start = rows_per_thread * i;
            final int stop = start + rows_per_thread;
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    // Create an empty VoltTable handle and then populate it in batches
                    // to be sent to the DBMS
                    VoltTable table = CatalogUtil.getVoltTable(catalog_tbl_a);
                    Object row[] = new Object[table.getColumnCount()];

                    for (int i = start; i < stop && i < init_record_count; i++) {
                        row[0] = i;

                        // randomly generate strings for each column
                        for (int col = 2; col < AffinityConstants.A_NUM_COLUMNS; col++) {
                            row[col] = YCSBUtil.astring(AffinityConstants.A_COLUMN_LENGTH, AffinityConstants.A_COLUMN_LENGTH);
                        } // FOR
                        table.addRow(row);

                        // insert this batch of tuples
                        if (table.getRowCount() >= AffinityConstants.BATCH_SIZE) {
                            loadVoltTable(AffinityConstants.TABLE_A_NAME, table);
                            total.addAndGet(table.getRowCount());
                            table.clearRowData();
                            if (debug.val)
                                LOG.debug(String.format("[%d] Records Loaded: %6d / %d",
                                          thread_id, total.get(), init_record_count));
                        }
                    } // FOR

                    // load remaining records
                    if (table.getRowCount() > 0) {
                        loadVoltTable(AffinityConstants.TABLE_A_NAME, table);
                        total.addAndGet(table.getRowCount());
                        table.clearRowData();
                        if (debug.val)
                            LOG.debug(String.format("[%d] Records Loaded: %6d / %d",
                                      thread_id, total.get(), init_record_count));
                    }
                }
            });
        } // FOR
        ThreadUtil.runGlobalPool(runnables);

        if (debug.val)
            LOG.info("Finished loading " + catalog_tbl_a.getName());

    }

}
