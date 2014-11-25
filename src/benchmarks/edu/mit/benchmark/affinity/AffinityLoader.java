package edu.mit.benchmark.affinity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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
    private final long init_supplier_count;
    private final long init_product_count;
    private final long init_part_count;
    private int loadthreads = ThreadUtil.availableProcessors();
    private Random rand;

    public static void main(String args[]) throws Exception {
        if (debug.val)
            LOG.debug("MAIN: " + AffinityLoader.class.getName());
        BenchmarkComponent.main(AffinityLoader.class, args, true);
    }
    
    public AffinityLoader(String[] args) {
        super(args);
        if (debug.val)
            LOG.debug("CONSTRUCTOR: " + AffinityLoader.class.getName());
        
        boolean useFixedSize = true;
        long num_suppliers = AffinityConstants.NUM_SUPPLIERS;
        long num_products = AffinityConstants.NUM_PRODUCTS;
        long num_parts = AffinityConstants.NUM_PARTS;
        rand = new Random();
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            // Used Fixed-size Database
            // Parameter that points to where we can find the initial data files
            if  (key.equalsIgnoreCase("num_suppliers")) {
                num_suppliers = Long.valueOf(value);
            }
            else if  (key.equalsIgnoreCase("num_products")) {
            	num_products = Long.valueOf(value);
            }
            else if  (key.equalsIgnoreCase("num_parts")) {
            	num_parts = Long.valueOf(value);
            }
            // Multi-Threaded Loader
            else if (key.equalsIgnoreCase("loadthreads")) {
                this.loadthreads = Integer.valueOf(value);
            }
        } // FOR
        
        // Figure out the # of records that we need
        if (useFixedSize) {
            this.init_supplier_count = num_suppliers;
            this.init_product_count = num_products;
            this.init_part_count = num_parts;
        }
        else {
            this.init_supplier_count = (int) Math.round(AffinityConstants.NUM_SUPPLIERS * this.getScaleFactor());
            this.init_product_count = (int) Math.round(AffinityConstants.NUM_PRODUCTS * this.getScaleFactor());
            this.init_part_count = (int) Math.round(AffinityConstants.NUM_PARTS * this.getScaleFactor());
        }
        LOG.info("Initializing database with " + init_supplier_count + " suppliers, " + 
        		+ init_product_count + " products, and " + init_part_count + 
        		" parts. Using " + this.loadthreads + " load threads");
    }

    @Override
    public void load() throws IOException {
        if (debug.val)
            LOG.debug("Starting AffinityLoader");

        final CatalogContext catalogContext = this.getCatalogContext(); 
        final Table catalog_tbl_suppliers = catalogContext.getTableByName(AffinityConstants.TABLENAME_SUPPLIERS);
        final AtomicLong total_suppliers = new AtomicLong(0);
        final AtomicLong total_products = new AtomicLong(0);
        final AtomicLong total_parts = new AtomicLong(0);
        final AtomicLong total_supplies = new AtomicLong(0);
        final AtomicLong total_uses = new AtomicLong(0);
        
        // Multi-threaded loader
        final int rows_per_thread = (int) Math.ceil(Math.max(init_supplier_count, Math.max(init_product_count, init_part_count)) / 
        		(double) this.loadthreads);
        final List<Runnable> runnables = new ArrayList<Runnable>();
        for (int i = 0; i < this.loadthreads; i++) {
            final int thread_id = i;
            final int start = rows_per_thread * i;
            final int stop = start + rows_per_thread;
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    loadSuppliers(thread_id, start, stop, total_suppliers);
                    loadProducts(thread_id, start, stop, total_products);
                    loadParts(thread_id, start, stop, total_parts);
                    loadSupplies(thread_id, start, stop, total_supplies);
                    loadUses(thread_id, start, stop, total_uses);
                }
            });
        } // FOR
        ThreadUtil.runGlobalPool(runnables);

        if (debug.val)
            LOG.info("Finished loading " + catalog_tbl_suppliers.getName());

    }
    
    public void loadSuppliers(int thread_id, int start, int stop, AtomicLong total) {
    	// Create an empty VoltTable handle and then populate it in batches
        // to be sent to the DBMS
    	final CatalogContext catalogContext = this.getCatalogContext(); 
        final Table catalog_tbl_suppliers = catalogContext.getTableByName(AffinityConstants.TABLENAME_SUPPLIERS);
        VoltTable table = CatalogUtil.getVoltTable(catalog_tbl_suppliers);
        Object row[] = new Object[table.getColumnCount()];

        for (int i = start; i < stop && i < init_supplier_count; i++) {
            row[0] = i;

            // randomly generate strings for each column
            for (int col = 2; col < AffinityConstants.SUPPLIERS_NUM_COLUMNS; col++) {
                row[col] = YCSBUtil.astring(AffinityConstants.SUPPLIERS_COLUMN_LENGTH, AffinityConstants.SUPPLIERS_COLUMN_LENGTH);
            } // FOR
            table.addRow(row);

            // insert this batch of tuples
            if (table.getRowCount() >= AffinityConstants.BATCH_SIZE) {
                loadVoltTable(AffinityConstants.TABLENAME_SUPPLIERS, table);
                total.addAndGet(table.getRowCount());
                table.clearRowData();
                if (debug.val)
                    LOG.debug(String.format("[%d] %s records Loaded: %6d / %d",
                              thread_id, AffinityConstants.TABLENAME_SUPPLIERS, total.get(), init_supplier_count));
            }
        } // FOR

        // load remaining records
        if (table.getRowCount() > 0) {
            loadVoltTable(AffinityConstants.TABLENAME_SUPPLIERS, table);
            total.addAndGet(table.getRowCount());
            table.clearRowData();
            if (debug.val)
                LOG.debug(String.format("[%d] %s records Loaded: %6d / %d",
                          thread_id, AffinityConstants.TABLENAME_SUPPLIERS, total.get(), init_supplier_count));
        }
    }
    
    public void loadProducts(int thread_id, int start, int stop, AtomicLong total) {
    	// Create an empty VoltTable handle and then populate it in batches
        // to be sent to the DBMS
    	final CatalogContext catalogContext = this.getCatalogContext(); 
        final Table catalog_tbl_products = catalogContext.getTableByName(AffinityConstants.TABLENAME_PRODUCTS);
        VoltTable table = CatalogUtil.getVoltTable(catalog_tbl_products);
        Object row[] = new Object[table.getColumnCount()];

        for (int i = start; i < stop && i < init_product_count; i++) {
            row[0] = i;

            // randomly generate strings for each column
            for (int col = 2; col < AffinityConstants.PRODUCTS_NUM_COLUMNS; col++) {
                row[col] = YCSBUtil.astring(AffinityConstants.PRODUCTS_COLUMN_LENGTH, AffinityConstants.PRODUCTS_COLUMN_LENGTH);
            } // FOR
            table.addRow(row);

            // insert this batch of tuples
            if (table.getRowCount() >= AffinityConstants.BATCH_SIZE) {
                loadVoltTable(AffinityConstants.TABLENAME_PRODUCTS, table);
                total.addAndGet(table.getRowCount());
                table.clearRowData();
                if (debug.val)
                    LOG.debug(String.format("[%d] %s records Loaded: %6d / %d",
                              thread_id, AffinityConstants.TABLENAME_PRODUCTS, total.get(), init_product_count));
            }
        } // FOR

        // load remaining records
        if (table.getRowCount() > 0) {
            loadVoltTable(AffinityConstants.TABLENAME_PRODUCTS, table);
            total.addAndGet(table.getRowCount());
            table.clearRowData();
            if (debug.val)
                LOG.debug(String.format("[%d] %s records Loaded: %6d / %d",
                          thread_id, AffinityConstants.TABLENAME_PRODUCTS, total.get(), init_product_count));
        }
    }

    
    public void loadParts(int thread_id, int start, int stop, AtomicLong total) {
    	// Create an empty VoltTable handle and then populate it in batches
        // to be sent to the DBMS
    	final CatalogContext catalogContext = this.getCatalogContext(); 
        final Table catalog_tbl_parts = catalogContext.getTableByName(AffinityConstants.TABLENAME_PARTS);
        VoltTable table = CatalogUtil.getVoltTable(catalog_tbl_parts);
        Object row[] = new Object[table.getColumnCount()];

        for (int i = start; i < stop && i < init_part_count; i++) {
            row[0] = i;

            // randomly generate strings for each column
            for (int col = 2; col < AffinityConstants.PARTS_NUM_COLUMNS; col++) {
                row[col] = YCSBUtil.astring(AffinityConstants.PARTS_COLUMN_LENGTH, AffinityConstants.PARTS_COLUMN_LENGTH);
            } // FOR
            table.addRow(row);

            // insert this batch of tuples
            if (table.getRowCount() >= AffinityConstants.BATCH_SIZE) {
                loadVoltTable(AffinityConstants.TABLENAME_PARTS, table);
                total.addAndGet(table.getRowCount());
                table.clearRowData();
                if (debug.val)
                    LOG.debug(String.format("[%d] %s records Loaded: %6d / %d",
                              thread_id, AffinityConstants.TABLENAME_PARTS, total.get(), init_part_count));
            }
        } // FOR

        // load remaining records
        if (table.getRowCount() > 0) {
            loadVoltTable(AffinityConstants.TABLENAME_PARTS, table);
            total.addAndGet(table.getRowCount());
            table.clearRowData();
            if (debug.val)
                LOG.debug(String.format("[%d] %s records Loaded: %6d / %d",
                          thread_id, AffinityConstants.TABLENAME_PARTS, total.get(), init_part_count));
        }
    }

    public void loadSupplies(int thread_id, int start, int stop, AtomicLong total) {
    	// Create an empty VoltTable handle and then populate it in batches
        // to be sent to the DBMS
    	final CatalogContext catalogContext = this.getCatalogContext(); 
        final Table catalog_tbl_supplies = catalogContext.getTableByName(AffinityConstants.TABLENAME_SUPPLIES);
        VoltTable table = CatalogUtil.getVoltTable(catalog_tbl_supplies);
        Object row[] = new Object[table.getColumnCount()];

        for (int i = start; i < stop && i < init_supplier_count; i++) {
            for(int j = start; j < stop && j < init_part_count; j++) {
            	if(rand.nextDouble() < AffinityConstants.SUPPLIES_PROBABILITY) {
            		row[0] = j;
            		row[1] = i;

            		table.addRow(row);

            		// insert this batch of tuples
            		if (table.getRowCount() >= AffinityConstants.BATCH_SIZE) {
            			loadVoltTable(AffinityConstants.TABLENAME_SUPPLIES, table);
            			total.addAndGet(table.getRowCount());
            			table.clearRowData();
            			if (debug.val)
            				LOG.debug(String.format("[%d] %s records Loaded: %6d",
            						thread_id, AffinityConstants.TABLENAME_SUPPLIES, total.get()));
            		}
            	}
            } // FOR
        } // FOR

        // load remaining records
        if (table.getRowCount() > 0) {
            loadVoltTable(AffinityConstants.TABLENAME_SUPPLIES, table);
            total.addAndGet(table.getRowCount());
            table.clearRowData();
            if (debug.val)
                LOG.debug(String.format("[%d] %s records Loaded: %6d",
                          thread_id, AffinityConstants.TABLENAME_SUPPLIES, total.get()));
        }
    }

    public void loadUses(int thread_id, int start, int stop, AtomicLong total) {
    	// Create an empty VoltTable handle and then populate it in batches
        // to be sent to the DBMS
    	final CatalogContext catalogContext = this.getCatalogContext(); 
        final Table catalog_tbl_uses = catalogContext.getTableByName(AffinityConstants.TABLENAME_USES);
        VoltTable table = CatalogUtil.getVoltTable(catalog_tbl_uses);
        Object row[] = new Object[table.getColumnCount()];

        for (int i = start; i < stop && i < init_product_count; i++) {
            for(int j = start; j < stop && j < init_part_count; j++) {
            	if(rand.nextDouble() < AffinityConstants.USES_PROBABILITY) {
            		row[0] = j;
            		row[1] = i;

            		table.addRow(row);

            		// insert this batch of tuples
            		if (table.getRowCount() >= AffinityConstants.BATCH_SIZE) {
            			loadVoltTable(AffinityConstants.TABLENAME_USES, table);
            			total.addAndGet(table.getRowCount());
            			table.clearRowData();
            			if (debug.val)
            				LOG.debug(String.format("[%d] %s records Loaded: %6d",
            						thread_id, AffinityConstants.TABLENAME_USES, total.get()));
            		}
            	}
            } // FOR
        } // FOR

        // load remaining records
        if (table.getRowCount() > 0) {
            loadVoltTable(AffinityConstants.TABLENAME_USES, table);
            total.addAndGet(table.getRowCount());
            table.clearRowData();
            if (debug.val)
                LOG.debug(String.format("[%d] %s records Loaded: %6d",
                          thread_id, AffinityConstants.TABLENAME_USES, total.get()));
        }
    }

}
