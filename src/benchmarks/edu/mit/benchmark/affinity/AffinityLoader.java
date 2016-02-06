package edu.mit.benchmark.affinity;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;

import edu.brown.api.BenchmarkComponent;
import edu.brown.api.Loader;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class AffinityLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(AffinityLoader.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }

    private AffinityConfig config;

    public static void main(String args[]) throws Exception {
        if (debug.val) {
            LOG.debug("MAIN: " + AffinityLoader.class.getName());
        }
        BenchmarkComponent.main(AffinityLoader.class, args, true);
    }

    public AffinityLoader(String[] args) {
        super(args);
        if (debug.val) {
            LOG.debug("CONSTRUCTOR: " + AffinityLoader.class.getName());
        }
        this.config = new AffinityConfig(this.m_extraParams);
        if (debug.val) {
            LOG.debug(this.config.toString());
        }
    }

    @Override
    public void load() throws IOException {
        if (debug.val) {
            LOG.debug("Starting AffinityLoader");
        }

        final AtomicLong total_suppliers = new AtomicLong(0);
        final AtomicLong total_products = new AtomicLong(0);
        final AtomicLong total_parts = new AtomicLong(0);
        final AtomicLong total_supplies = new AtomicLong(0);
        final AtomicLong total_uses = new AtomicLong(0);

        // Multi-threaded loader
        // final int rows_per_thread = (int)
        // Math.ceil(Math.max(config.num_suppliers,
        // Math.max(config.num_products, config.num_parts)) /
        // (double) config.loadthreads);
        // final List<Runnable> runnables = new ArrayList<Runnable>();
        // for (int i = 0; i < config.loadthreads; i++) {
        // final int thread_id = i;
        // final int start = rows_per_thread * i;
        // final int stop = start + rows_per_thread;
        // runnables.add(new Runnable() {
        // @Override
        // public void run() {
        // loadSuppliers(thread_id, start, stop, total_suppliers);
        // loadProducts(thread_id, start, stop, total_products);
        // loadParts(thread_id, start, stop, total_parts);
        // loadSupplies(thread_id, start, stop, total_supplies);
        // loadUses(thread_id, start, stop, total_uses);
        // }
        // });
        // } // FOR
        // ThreadUtil.runGlobalPool(runnables);

        this.loadSuppliers(0, 0, this.config.num_suppliers, total_suppliers);
        this.loadProducts(0, 0, this.config.num_products, total_products);
        this.loadParts(0, 0, this.config.num_parts, total_parts);
        this.loadSupplies(0, 0, this.config.num_suppliers, total_supplies);
        this.loadUses(0, 0, this.config.num_products, total_uses);

    }

    public void loadSuppliers(int thread_id, long start, long stop, AtomicLong total) {
        // Create an empty VoltTable handle and then populate it in batches
        // to be sent to the DBMS
        final CatalogContext catalogContext = this.getCatalogContext();
        final Table catalog_tbl_suppliers = catalogContext.getTableByName(AffinityConstants.TABLENAME_SUPPLIERS);
        VoltTable table = org.voltdb.utils.CatalogUtil.getVoltTable(catalog_tbl_suppliers);
        Object row[] = new Object[table.getColumnCount()];

        byte[] padding = new byte[AffinityConstants.SUPPLIERS_COLUMN_LENGTH];
        for (int i = 0; i < AffinityConstants.SUPPLIERS_COLUMN_LENGTH; ++i) {
            padding[i] = 'a';
        }
        String paddingString = new String(padding);

        for (long i = start; i < stop && i < this.config.num_suppliers; i++) {
            row[0] = i;

            // randomly generate strings for each column
            for (int col = 1; col < AffinityConstants.SUPPLIERS_NUM_COLUMNS; col++) {
                // row[col] =
                // YCSBUtil.astring(AffinityConstants.SUPPLIERS_COLUMN_LENGTH,
                // AffinityConstants.SUPPLIERS_COLUMN_LENGTH);
                row[col] = paddingString;
                // TODO do the same for the other tables
            } // FOR
            table.addRow(row);

            // insert this batch of tuples
            // if ((i + 1) % AffinityConstants.BATCH_SIZE == 0 || (i + 1) ==
            // config.num_suppliers) {
            if (table.getRowCount() >= AffinityConstants.BATCH_SIZE) {
                if (debug.val) {
                    LOG.debug(String.format("%tT [Worker %d] Loading %s records: %6d - %d / %d", Calendar.getInstance(), thread_id, AffinityConstants.TABLENAME_SUPPLIERS, total.get(), total.get()
                            + table.getRowCount(), this.config.num_suppliers));
                }
                this.loadVoltTable(AffinityConstants.TABLENAME_SUPPLIERS, table);

                total.addAndGet(table.getRowCount());
                table.clearRowData();
                if (debug.val) {
                    LOG.debug(String.format("[%d] %s records Loaded: %6d / %d", thread_id, AffinityConstants.TABLENAME_SUPPLIERS, total.get(), this.config.num_suppliers));
                }
            }
        } // FOR

        // // load remaining records
        if (table.getRowCount() > 0) {
            if (debug.val) {
                LOG.debug(String.format("%tT [Worker %d] Loading %s records: remaining", Calendar.getInstance(), thread_id, AffinityConstants.TABLENAME_SUPPLIERS));
            }

            this.loadVoltTable(AffinityConstants.TABLENAME_SUPPLIERS, table);

            total.addAndGet(table.getRowCount());
            table.clearRowData();
            if (debug.val) {
                LOG.debug(String.format("[%d] %s records Loaded: %6d / %d", thread_id, AffinityConstants.TABLENAME_SUPPLIERS, total.get(), this.config.num_suppliers));
            }
        }
    }

    public void loadProducts(int thread_id, long start, long stop, AtomicLong total) {
        // Create an empty VoltTable handle and then populate it in batches
        // to be sent to the DBMS
        final CatalogContext catalogContext = this.getCatalogContext();
        final Table catalog_tbl_products = catalogContext.getTableByName(AffinityConstants.TABLENAME_PRODUCTS);
        VoltTable table = org.voltdb.utils.CatalogUtil.getVoltTable(catalog_tbl_products);
        Object row[] = new Object[table.getColumnCount()];

        byte[] padding = new byte[AffinityConstants.SUPPLIERS_COLUMN_LENGTH];
        for (int i = 0; i < AffinityConstants.SUPPLIERS_COLUMN_LENGTH; ++i) {
            padding[i] = 'a';
        }
        String paddingString = new String(padding);

        for (long i = start; i < stop && i < this.config.num_products; i++) {
            row[0] = i;

            // randomly generate strings for each column
            for (int col = 1; col < AffinityConstants.PRODUCTS_NUM_COLUMNS; col++) {
                // row[col] =
                // YCSBUtil.astring(AffinityConstants.PRODUCTS_COLUMN_LENGTH,
                // AffinityConstants.PRODUCTS_COLUMN_LENGTH);
                row[col] = paddingString;
            } // FOR
            table.addRow(row);

            // insert this batch of tuples
            // if ((i + 1) % AffinityConstants.BATCH_SIZE == 0 || (i + 1) ==
            // config.num_products) {
            if (table.getRowCount() >= AffinityConstants.BATCH_SIZE) {
                if (debug.val) {
                    LOG.debug(String.format("%tT [Worker %d] Loading %s records: %6d - %d / %d", Calendar.getInstance(), thread_id, AffinityConstants.TABLENAME_PRODUCTS, total.get(), total.get()
                            + table.getRowCount(), this.config.num_products));
                }

                this.loadVoltTable(AffinityConstants.TABLENAME_PRODUCTS, table);

                total.addAndGet(table.getRowCount());
                table.clearRowData();
                if (debug.val) {
                    LOG.debug(String.format("[%d] %s records Loaded: %6d / %d", thread_id, AffinityConstants.TABLENAME_PRODUCTS, total.get(), this.config.num_products));
                }
            }
        } // FOR

        // load remaining records
        if (table.getRowCount() > 0) {
            if (debug.val) {
                LOG.debug(String.format("%tT [Worker %d] Loading %s records: remaining", Calendar.getInstance(), thread_id, AffinityConstants.TABLENAME_PRODUCTS));
            }

            this.loadVoltTable(AffinityConstants.TABLENAME_PRODUCTS, table);

            total.addAndGet(table.getRowCount());
            table.clearRowData();
            if (debug.val) {
                LOG.debug(String.format("[%d] %s records Loaded: %6d / %d", thread_id, AffinityConstants.TABLENAME_PRODUCTS, total.get(), this.config.num_products));
            }
        }
    }

    public void loadParts(int thread_id, long start, long stop, AtomicLong total) {
        // Create an empty VoltTable handle and then populate it in batches
        // to be sent to the DBMS
        final CatalogContext catalogContext = this.getCatalogContext();
        final Table catalog_tbl_parts = catalogContext.getTableByName(AffinityConstants.TABLENAME_PARTS);
        VoltTable table = org.voltdb.utils.CatalogUtil.getVoltTable(catalog_tbl_parts);
        Object row[] = new Object[table.getColumnCount()];

        byte[] padding = new byte[AffinityConstants.SUPPLIERS_COLUMN_LENGTH];
        for (int i = 0; i < AffinityConstants.SUPPLIERS_COLUMN_LENGTH; ++i) {
            padding[i] = 'a';
        }
        String paddingString = new String(padding);

        for (long i = start; i < stop && i < this.config.num_parts; i++) {
            row[0] = i;

            // randomly generate strings for each column
            for (int col = 1; col < AffinityConstants.PARTS_NUM_COLUMNS; col++) {
                // row[col] =
                // YCSBUtil.astring(AffinityConstants.PARTS_COLUMN_LENGTH,
                // AffinityConstants.PARTS_COLUMN_LENGTH);
                row[col] = paddingString;
            } // FOR
            table.addRow(row);

            // insert this batch of tuples
            if (table.getRowCount() >= AffinityConstants.BATCH_SIZE) {
                if (debug.val) {
                    LOG.debug(String.format("%tT [Worker %d] Loading %s records: %6d - %d / %d", Calendar.getInstance(), thread_id, AffinityConstants.TABLENAME_PARTS, total.get(),
                            total.get() + table.getRowCount(), this.config.num_parts));
                }

                this.loadVoltTable(AffinityConstants.TABLENAME_PARTS, table);

                total.addAndGet(table.getRowCount());
                table.clearRowData();
                if (debug.val) {
                    LOG.debug(String.format("[%d] %s records loaded: %6d / %d", thread_id, AffinityConstants.TABLENAME_PARTS, total.get(), this.config.num_parts));
                }
            }
        } // FOR

        // load remaining records
        if (table.getRowCount() > 0) {
            if (debug.val) {
                LOG.debug(String.format("%tT [Worker %d] Loading %s records: remaining", Calendar.getInstance(), thread_id, AffinityConstants.TABLENAME_PARTS));
            }

            this.loadVoltTable(AffinityConstants.TABLENAME_PARTS, table);

            total.addAndGet(table.getRowCount());
            table.clearRowData();
            if (debug.val) {
                LOG.debug(String.format("[%d] %s records Loaded: %6d / %d", thread_id, AffinityConstants.TABLENAME_PARTS, total.get(), this.config.num_parts));
            }
        }
    }

    public void loadSupplies(int thread_id, long start, long stop, AtomicLong total) {
        // Create an empty VoltTable handle and then populate it in batches
        // to be sent to the DBMS
        final CatalogContext catalogContext = this.getCatalogContext();
        final Table catalog_tbl_supplies = catalogContext.getTableByName(AffinityConstants.TABLENAME_SUPPLIES);
        VoltTable table = org.voltdb.utils.CatalogUtil.getVoltTable(catalog_tbl_supplies);
        Object row[] = new Object[table.getColumnCount()];

        AffinityGenerator supplies_gen = this.config.getAffinityGenerator(AffinityConstants.SUPPLIES_PRE, this.config.num_parts, this.m_extraParams);
        LOG.info("supplies_gen : " + supplies_gen);

        for (long i = start; i < stop && i < this.config.num_suppliers; i++) {
            HashSet<Integer> parts = new HashSet<Integer>();
            double shift = 0;
            if (this.config.supplierToPartsRandomOffset) {
                shift = this.config.rand_gen.nextDouble();
            } else {
                shift = (((double) i) / this.config.num_suppliers + this.config.supplierToPartsOffset) % 1.0;
            }
            supplies_gen.resetLastItem();
            for (int j = 0; j < this.config.max_parts_per_supplier; j++) {
                parts.add(supplies_gen.nextInt(shift));
            }
            for (Integer part : parts) {
                row[0] = part;
                row[1] = i;

                table.addRow(row);

                // insert this batch of tuples
                // if ((i + 1) % AffinityConstants.BATCH_SIZE == 0 || (i + 1) ==
                // config.max_parts_per_supplier) {
                if (table.getRowCount() >= AffinityConstants.BATCH_SIZE) {
                    if (debug.val) {
                        LOG.debug(String.format("%tT [Worker %d] Loading %s records: %6d - %d / %d", Calendar.getInstance(), thread_id, AffinityConstants.TABLENAME_SUPPLIES, total.get(), total.get()
                                + table.getRowCount(), this.config.num_suppliers * this.config.max_parts_per_supplier));
                    }

                    this.loadVoltTable(AffinityConstants.TABLENAME_SUPPLIES, table);

                    total.addAndGet(table.getRowCount());
                    table.clearRowData();
                    if (debug.val) {
                        LOG.debug(String.format("[%d] %s records loaded: %6d", thread_id, AffinityConstants.TABLENAME_SUPPLIES, total.get()));
                    }
                }
            } // FOR
        } // FOR

        // load remaining records
        if (table.getRowCount() > 0) {
            if (debug.val) {
                LOG.debug(String.format("%tT [Worker %d] Loading %s records: remaining", Calendar.getInstance(), thread_id, AffinityConstants.TABLENAME_SUPPLIES));
            }

            this.loadVoltTable(AffinityConstants.TABLENAME_SUPPLIES, table);

            total.addAndGet(table.getRowCount());
            table.clearRowData();
            if (debug.val) {
                LOG.debug(String.format("[%d] %s records Loaded: %6d", thread_id, AffinityConstants.TABLENAME_SUPPLIES, total.get()));
            }
        }
    }

    public void loadUses(int thread_id, long start, long stop, AtomicLong total) {
        // Create an empty VoltTable handle and then populate it in batches
        // to be sent to the DBMS
        final CatalogContext catalogContext = this.getCatalogContext();
        final Table catalog_tbl_uses = catalogContext.getTableByName(AffinityConstants.TABLENAME_USES);
        VoltTable table = org.voltdb.utils.CatalogUtil.getVoltTable(catalog_tbl_uses);
        Object row[] = new Object[table.getColumnCount()];

        AffinityGenerator uses_gen = this.config.getAffinityGenerator(AffinityConstants.USES_PRE, this.config.num_parts, this.m_extraParams);
        LOG.info("uses_gen : " + uses_gen);

        for (long i = start; i < stop && i < this.config.num_products; i++) {
            HashSet<Integer> parts = new HashSet<Integer>();
            double shift = 0;
            if (this.config.productToPartsRandomOffset) {
                shift = this.config.rand_gen.nextDouble();
            } else {
                shift = (((double) i) / this.config.num_products + this.config.productToPartsOffset) % 1.0;
            }
            uses_gen.resetLastItem();
            for (int j = 0; j < this.config.max_parts_per_product; j++) {
                parts.add(uses_gen.nextInt(shift));
            }
            for (Integer part : parts) {
                row[0] = part;
                row[1] = i;

                table.addRow(row);

                // insert this batch of tuples
                if (table.getRowCount() >= AffinityConstants.BATCH_SIZE) {
                    if (debug.val) {
                        LOG.debug(String.format("%tT [Worker %d] Loading %s records: %6d - %d / %d", Calendar.getInstance(), thread_id, AffinityConstants.TABLENAME_USES, total.get(), total.get()
                                + table.getRowCount(), this.config.num_products * this.config.max_parts_per_product));
                    }
                    this.loadVoltTable(AffinityConstants.TABLENAME_USES, table);
                    total.addAndGet(table.getRowCount());

                    table.clearRowData();
                    if (debug.val) {
                        LOG.debug(String.format("[%d] %s records Loaded: %6d", thread_id, AffinityConstants.TABLENAME_USES, total.get()));
                    }
                }
            } // FOR
        } // FOR

        // load remaining records
        if (table.getRowCount() > 0) {
            if (debug.val) {
                LOG.debug(String.format("%tT [Worker %d] Loading %s records: remaining", Calendar.getInstance(), thread_id, AffinityConstants.TABLENAME_USES));
            }

            this.loadVoltTable(AffinityConstants.TABLENAME_USES, table);

            total.addAndGet(table.getRowCount());
            table.clearRowData();
            if (debug.val) {
                LOG.debug(String.format("[%d] %s records Loaded: %6d", thread_id, AffinityConstants.TABLENAME_USES, total.get()));
            }
        }
    }

}
