package edu.mit.benchmark.affinity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
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

    private AffinityConfig config;

    public static void main(String args[]) throws Exception {
        if (debug.val)
            LOG.debug("MAIN: " + AffinityLoader.class.getName());
        BenchmarkComponent.main(AffinityLoader.class, args, true);
    }
    
    public AffinityLoader(String[] args) {
        super(args);
        if (debug.val)
            LOG.debug("CONSTRUCTOR: " + AffinityLoader.class.getName());
        config = new AffinityConfig(m_extraParams);
        if (debug.val)
            LOG.debug(config.toString());
    }

    @Override
    public void load() throws IOException {
        if (debug.val)
            LOG.debug("Starting AffinityLoader");

        final AtomicLong total_suppliers = new AtomicLong(0);
        final AtomicLong total_products = new AtomicLong(0);
        final AtomicLong total_parts = new AtomicLong(0);
        final AtomicLong total_supplies = new AtomicLong(0);
        final AtomicLong total_uses = new AtomicLong(0);
        
        // Multi-threaded loader
        final int rows_per_thread = (int) Math.ceil(Math.max(config.num_suppliers, Math.max(config.num_products, config.num_parts)) / 
        		(double) config.loadthreads);
        final List<Runnable> runnables = new ArrayList<Runnable>();
        for (int i = 0; i < config.loadthreads; i++) {
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

    }
    
    public void loadSuppliers(int thread_id, int start, int stop, AtomicLong total) {
    	// Create an empty VoltTable handle and then populate it in batches
        // to be sent to the DBMS
    	final CatalogContext catalogContext = this.getCatalogContext(); 
        final Table catalog_tbl_suppliers = catalogContext.getTableByName(AffinityConstants.TABLENAME_SUPPLIERS);
        VoltTable table = CatalogUtil.getVoltTable(catalog_tbl_suppliers);
        Object row[] = new Object[table.getColumnCount()];

        for (int i = start; i < stop && i < config.num_suppliers; i++) {
            row[0] = i;

            // randomly generate strings for each column
            for (int col = 1; col < AffinityConstants.SUPPLIERS_NUM_COLUMNS; col++) {
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
                              thread_id, AffinityConstants.TABLENAME_SUPPLIERS, total.get(), config.num_suppliers));
            }
        } // FOR

        // load remaining records
        if (table.getRowCount() > 0) {
            loadVoltTable(AffinityConstants.TABLENAME_SUPPLIERS, table);
            total.addAndGet(table.getRowCount());
            table.clearRowData();
            if (debug.val)
                LOG.debug(String.format("[%d] %s records Loaded: %6d / %d",
                          thread_id, AffinityConstants.TABLENAME_SUPPLIERS, total.get(), config.num_suppliers));
        }
    }
    
    public void loadProducts(int thread_id, int start, int stop, AtomicLong total) {
    	// Create an empty VoltTable handle and then populate it in batches
        // to be sent to the DBMS
    	final CatalogContext catalogContext = this.getCatalogContext(); 
        final Table catalog_tbl_products = catalogContext.getTableByName(AffinityConstants.TABLENAME_PRODUCTS);
        VoltTable table = CatalogUtil.getVoltTable(catalog_tbl_products);
        Object row[] = new Object[table.getColumnCount()];

        for (int i = start; i < stop && i < config.num_products; i++) {
            row[0] = i;

            // randomly generate strings for each column
            for (int col = 1; col < AffinityConstants.PRODUCTS_NUM_COLUMNS; col++) {
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
                              thread_id, AffinityConstants.TABLENAME_PRODUCTS, total.get(), config.num_products));
            }
        } // FOR

        // load remaining records
        if (table.getRowCount() > 0) {
            loadVoltTable(AffinityConstants.TABLENAME_PRODUCTS, table);
            total.addAndGet(table.getRowCount());
            table.clearRowData();
            if (debug.val)
                LOG.debug(String.format("[%d] %s records Loaded: %6d / %d",
                          thread_id, AffinityConstants.TABLENAME_PRODUCTS, total.get(), config.num_products));
        }
    }

    
    public void loadParts(int thread_id, int start, int stop, AtomicLong total) {
    	// Create an empty VoltTable handle and then populate it in batches
        // to be sent to the DBMS
    	final CatalogContext catalogContext = this.getCatalogContext(); 
        final Table catalog_tbl_parts = catalogContext.getTableByName(AffinityConstants.TABLENAME_PARTS);
        VoltTable table = CatalogUtil.getVoltTable(catalog_tbl_parts);
        Object row[] = new Object[table.getColumnCount()];

        for (int i = start; i < stop && i < config.num_parts; i++) {
            row[0] = i;

            // randomly generate strings for each column
            for (int col = 1; col < AffinityConstants.PARTS_NUM_COLUMNS; col++) {
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
                              thread_id, AffinityConstants.TABLENAME_PARTS, total.get(), config.num_parts));
            }
        } // FOR

        // load remaining records
        if (table.getRowCount() > 0) {
            loadVoltTable(AffinityConstants.TABLENAME_PARTS, table);
            total.addAndGet(table.getRowCount());
            table.clearRowData();
            if (debug.val)
                LOG.debug(String.format("[%d] %s records Loaded: %6d / %d",
                          thread_id, AffinityConstants.TABLENAME_PARTS, total.get(), config.num_parts));
        }
    }

    public void loadSupplies(int thread_id, int start, int stop, AtomicLong total) {
    	// Create an empty VoltTable handle and then populate it in batches
        // to be sent to the DBMS
    	final CatalogContext catalogContext = this.getCatalogContext(); 
        final Table catalog_tbl_supplies = catalogContext.getTableByName(AffinityConstants.TABLENAME_SUPPLIES);
        VoltTable table = CatalogUtil.getVoltTable(catalog_tbl_supplies);
        Object row[] = new Object[table.getColumnCount()];

        for (int i = start; i < stop && i < config.num_suppliers; i++) {
        	HashSet<Integer> parts = new HashSet<Integer>();
        	double shift = 0;
        	if(config.supplierToPartsRandomOffset) {
        		shift = config.rand_gen.nextDouble();
        	}
        	else {
        		shift = (((double) i)/config.num_suppliers + config.supplierToPartsOffset) % 1.0;
        	}
        	config.supplies_gen.resetLastItem();
        	for(int j = 0; j < config.MAX_PARTS_PER_SUPPLIER; j++) { 
        		parts.add(config.supplies_gen.nextInt(shift));
        	}
        	for(Integer part : parts) {
        		row[0] = part;
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

        for (int i = start; i < stop && i < config.num_products; i++) {
        	HashSet<Integer> parts = new HashSet<Integer>();
        	double shift = 0;
        	if(config.productToPartsRandomOffset) {
        		shift = config.rand_gen.nextDouble();
        	}
        	else {
        		shift = (((double) i)/config.num_products + config.productToPartsOffset) % 1.0;
        	}
        	config.uses_gen.resetLastItem();
        	for(int j = 0; j < config.MAX_PARTS_PER_PRODUCT; j++) { 
        		parts.add(config.uses_gen.nextInt(shift));
        	}
        	for(Integer part : parts) {
        		row[0] = part;
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
