package edu.mit.benchmark.b2w;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import edu.brown.catalog.CatalogUtil;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import edu.brown.api.BenchmarkComponent;
import edu.brown.api.Loader;
import edu.brown.hashing.MurmurHash;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ThreadUtil;

import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;

public class B2WLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(B2WLoader.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    public static enum KeyType {
        SKU,
        STOCK_ID,
        TRANSACTION_ID,
        CART_ID,
        CHECKOUT_ID;
    } // KeyType ENUM


    public final static int configCommitCount = 500;


    private final static int KEY_TYPE_INTEGER = 0;

    private final static int KEY_TYPE_VARCHAR = 1;

    private final static int KEY_TYPE_BIGINT = 2;

    private final static int KEY_TYPE_TINYINT = 3;

    private final static int KEY_TYPE_FLOAT = 4;

    private final static int KEY_TYPE_TIMESTAMP = 5;

    private final static byte TINY_INT_TRUE = 1;

    private final static byte TINY_INT_FALSE = 0;

    private final static int[] STK_INVENTORY_STOCK_TYPES = {
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_INTEGER,
            KEY_TYPE_INTEGER,
            KEY_TYPE_INTEGER,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_INTEGER,
    };
    private final static int[] STK_INVENTORY_STOCK_QUANTITY_TYPES = {
            KEY_TYPE_VARCHAR,
            KEY_TYPE_INTEGER,
            KEY_TYPE_INTEGER,
            KEY_TYPE_INTEGER,
    };
    private final static int[] STK_STOCK_TRANSACTION_TYPES = {
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_TIMESTAMP,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_TIMESTAMP,
            KEY_TYPE_TINYINT,
            KEY_TYPE_INTEGER,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_INTEGER,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_INTEGER,
            KEY_TYPE_INTEGER,
    };
    private final static int[] CART_TYPES = {
            KEY_TYPE_VARCHAR,
            KEY_TYPE_FLOAT,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_TIMESTAMP,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_TINYINT,
    };
    private final static int[] CART_CUSTOMER_TYPES = {
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_TINYINT,
            KEY_TYPE_TINYINT,
    };
    private final static int[] CART_LINES_TYPES = {
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_BIGINT,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_FLOAT,
            KEY_TYPE_FLOAT,
            KEY_TYPE_INTEGER,
            KEY_TYPE_INTEGER,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_INTEGER,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_TIMESTAMP,
    };
    private final static int[] CART_LINE_PRODUCTS_TYPES = {
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_BIGINT,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_TINYINT,
            KEY_TYPE_FLOAT,
            KEY_TYPE_FLOAT,
            KEY_TYPE_TINYINT,
            KEY_TYPE_BIGINT,
            KEY_TYPE_BIGINT,
            KEY_TYPE_BIGINT,
            KEY_TYPE_FLOAT,
            KEY_TYPE_BIGINT,
    };
    private final static int[] CART_LINE_PROMOTIONS_TYPES = {
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_FLOAT,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_TINYINT,
            KEY_TYPE_FLOAT,
    };
    private final static int[] CART_LINE_PRODUCT_WARRANTIES_TYPES = {
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
    };
    private final static int[] CART_LINE_PRODUCT_STORES_TYPES = {
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
    };
    private final static int[] CHECKOUT_TYPES = {
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_FLOAT,
            KEY_TYPE_FLOAT,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_FLOAT,
            KEY_TYPE_VARCHAR,
    };
    private final static int[] CHECKOUT_PAYMENTS_TYPES = {
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_INTEGER,
            KEY_TYPE_FLOAT,
            KEY_TYPE_INTEGER,
            KEY_TYPE_FLOAT,
            KEY_TYPE_INTEGER,
            KEY_TYPE_INTEGER,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
    };
    private final static int[] CHECKOUT_FREIGHT_DELIVERY_TIME_TYPES = {
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_INTEGER,
    };
    private final static int[] CHECKOUT_STOCK_TRANSACTIONS_TYPES = {
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
    };

    private B2WConfig config;
    
    public static void main(String args[]) throws Exception {
        if (debug.val) {
            LOG.debug("MAIN: " + B2WLoader.class.getName());
        }
        BenchmarkComponent.main(B2WLoader.class, args, true);
    }

    public B2WLoader(String[] args) {
        super(args);
        if (debug.val) {
            LOG.debug("CONSTRUCTOR: " + B2WLoader.class.getName());
        }
        this.config = new B2WConfig(this.m_extraParams);
        if (debug.val) {
            LOG.debug(this.config.toString());
        }
    }


    /**
     * This function is used to translate the string value into specific type which could be
     * accepted by VoltType.
     * @param value the original value we want to translate.
     * @param type the target type we want to get.
     * @return return the target object we want, if the value is null or the type is invalid, then return null.
     */
    private Object getDataByType(String value, int type){
        if (value.equals("null") || value.equals("<null>") || value.equals(""))
            return null;
        switch (type){
            case KEY_TYPE_INTEGER:
                return Integer.parseInt(value);
            case KEY_TYPE_VARCHAR:
                return value;
            case KEY_TYPE_BIGINT:
                return Long.parseLong(value);
            case KEY_TYPE_TINYINT:
                return Boolean.parseBoolean(value)?TINY_INT_TRUE:TINY_INT_FALSE;
            case KEY_TYPE_FLOAT:
                return Double.parseDouble(value);
            case KEY_TYPE_TIMESTAMP:
                SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
                SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
                Date date;
                try {
                    date = format1.parse(value);
                } catch (ParseException e1) {
                    try {
                        date = format2.parse(value);
                    } catch (ParseException e2) {
                        LOG.error("invalid timestamp " + value);
                        date = new Date();
                    }
                }
                
                return new TimestampType(date);
            default:
                return null;
        }
    }

    @Override
    public ClientResponse loadVoltTable(String tableName, VoltTable vt){
        if (vt.getRowSize() == 0)
            return null;
        else
            return super.loadVoltTable(tableName, vt);
    }


    public static Integer hashPartition(String s, KeyType t){
        if (s == null)
            return null;
        else {
            switch(t) {
                case SKU:
                    return hashPartitionSku(s);
                case STOCK_ID:
                    return hashPartitionStockId(s);
                case TRANSACTION_ID:
                    return hashPartitionTransactionId(s);
                case CART_ID:
                    return hashPartitionCartId(s);
                case CHECKOUT_ID:
                    return hashPartitionCheckoutId(s);
                default:
                   throw new RuntimeException("Unrecognized key type " + t);     
            }
        }        
    }

    public static Integer hashPartitionSku(String s){
        if (s == null)
            return null;
        else {
            return (MurmurHash.hash32(s, 3292) % B2WConstants.NUM_KEYS + B2WConstants.NUM_KEYS) % B2WConstants.NUM_KEYS;
        }        
    }

    public static Integer hashPartitionStockId(String s){
        if (s == null)
            return null;
        else {
            return (MurmurHash.hash32(s, 977) % B2WConstants.NUM_KEYS + B2WConstants.NUM_KEYS) % B2WConstants.NUM_KEYS;
        }        
    }

    public static Integer hashPartitionTransactionId(String s){
        if (s == null)
            return null;
        else {
            return (MurmurHash.hash32(s, 2444) % B2WConstants.NUM_KEYS + B2WConstants.NUM_KEYS) % B2WConstants.NUM_KEYS;
        }        
    }

    public static Integer hashPartitionCartId(String s){
        if (s == null)
            return null;
        else {
            return (MurmurHash.hash32(s, 3837) % B2WConstants.NUM_KEYS + B2WConstants.NUM_KEYS) % B2WConstants.NUM_KEYS;
        }        
    }

    public static Integer hashPartitionCheckoutId(String s){
        if (s == null)
            return null;
        else {
            return (MurmurHash.hash32(s, 3837) % B2WConstants.NUM_KEYS + B2WConstants.NUM_KEYS) % B2WConstants.NUM_KEYS;
        }        
    }

    /**
     * This function is used to read the file has the format like :
     * sku;id;warehouse;sub_inventory;stock_type;store_id;lead_time
     * e.g.:
     * 163213;f9705d24-a125-4429-8626-16fc98cfe784;77;1001;1;B2W;1
     * and load it to the VoltTable.
     * @param thread_id the id of this worker thread
     * @param name the name of the table we want to modified.
     * @param path the path of the file we want to read.
     * @param types the types list of the table, an example is this.STOCK_TRANSACTION_TYPES
     * @param separator the separator in each line. In the example the separator is ";"
     * @param total the total number of records loaded for this table by all threads 
     */
    private void loadTableFormatData(int thread_id, String name, String path, int[] types, 
            String separator, AtomicLong total, KeyType keyType) {
        try {
            final CatalogContext catalogContext = this.getCatalogContext();
            Database catalog_db = catalogContext.database;
            Table catalog_tbl = catalog_db.getTables().getIgnoreCase(name);
            assert(catalog_tbl != null);
            VoltTable table = CatalogUtil.getVoltTable(catalog_tbl);
            int num_cols = catalog_tbl.getColumns().size();

            String line;
            LoaderSelector load_selector;

            try {
                load_selector = LoaderSelector.getLoaderSelector(path, this.config);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }

            while (true){
                try {
                    line = load_selector.nextLine();
                } catch(IOException e) {
                    throw new RuntimeException(e);
                }
                if (line == null)
                    break;
                String[] items = line.split(separator, -1);
                if (items.length != num_cols-1) {
                    LOG.error(name + " line has " + items.length + " items, expected " + (num_cols-1) + ": " + line);
                    continue;
                }

                Object row[] = new Object[num_cols];
                int param;
                row[0] = hashPartition((String) getDataByType(items[0], types[0]), keyType);
                for (param = 0; param < num_cols - 1; param++){
                    row[param + 1] = getDataByType(items[param], types[param]);
                }
                // Hack to increase the amount of available product
                if (name.equals(B2WConstants.TABLENAME_INVENTORY_STOCK_QUANTITY)) {
                    final int AVAILABLE = 2;
                    row[AVAILABLE] = (Integer) row[AVAILABLE] * 1000;
                }
                table.addRow(row);

                if (table.getRowCount() >= B2WConstants.BATCH_SIZE) {
                    this.loadVoltTable(catalog_tbl.getName(), table);

                    long current_total = total.addAndGet(table.getRowCount());
                    table.clearRowData();

                    if (debug.val)
                        LOG.debug("[Worker " + thread_id + "] " + name + " records loaded: " + current_total);
                    else if ((current_total % 100000) == 0) {
                        LOG.info("[Worker " + thread_id + "] " + name + " records loaded: " + current_total);
                    }
                }
            }

            if (table.getRowCount() > 0) {
                this.loadVoltTable(catalog_tbl.getName(), table);

                total.addAndGet(table.getRowCount());
                table.clearRowData();
            }

            if (LOG.isDebugEnabled()) LOG.debug("[" + name + " Loaded] "+ total.get());
        } catch(Exception e) {
            LOG.error("Error loading table " + name + " from file " + path + ": " + e.getMessage());
        }
    }

    @Override
    public void load() throws IOException {
        LOG.info("Starting B2WLoader");

        final AtomicLong total_stk_inventory_stock = new AtomicLong(0);
        final AtomicLong total_stk_inventory_stock_quantity = new AtomicLong(0);
        final AtomicLong total_stk_stock_transaction = new AtomicLong(0);
        final AtomicLong total_cart = new AtomicLong(0);
        final AtomicLong total_cart_customer = new AtomicLong(0);
        final AtomicLong total_cart_lines = new AtomicLong(0);
        final AtomicLong total_cart_line_products = new AtomicLong(0);
        final AtomicLong total_cart_line_promotions = new AtomicLong(0);
        final AtomicLong total_cart_line_product_warranties = new AtomicLong(0);
        final AtomicLong total_cart_line_product_stores = new AtomicLong(0);
        final AtomicLong total_checkout = new AtomicLong(0);
        final AtomicLong total_checkout_payments = new AtomicLong(0);
        final AtomicLong total_checkout_freight_delivery_time = new AtomicLong(0);
        final AtomicLong total_checkout_stock_transactions = new AtomicLong(0);

        // Multi-threaded loader
        final List<Runnable> runnables = new ArrayList<Runnable>();
        for (int i = 0; i < config.loadthreads; i++) {
            final int thread_id = i;
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    loadTableFormatData(thread_id, B2WConstants.TABLENAME_INVENTORY_STOCK,
                            config.STK_INVENTORY_STOCK_DATA_FILE, STK_INVENTORY_STOCK_TYPES, ",", total_stk_inventory_stock, KeyType.SKU);
                    loadTableFormatData(thread_id, B2WConstants.TABLENAME_INVENTORY_STOCK_QUANTITY,
                            config.STK_INVENTORY_STOCK_QUANTITY_DATA_FILE, STK_INVENTORY_STOCK_QUANTITY_TYPES, ",", total_stk_inventory_stock_quantity, KeyType.STOCK_ID);
                    loadTableFormatData(thread_id, B2WConstants.TABLENAME_STOCK_TRANSACTION,
                            config.STK_STOCK_TRANSACTION_DATA_FILE, STK_STOCK_TRANSACTION_TYPES, ";", total_stk_stock_transaction, KeyType.TRANSACTION_ID); // different separator
                    loadTableFormatData(thread_id, B2WConstants.TABLENAME_CART,
                            config.CART_DATA_FILE, CART_TYPES, ";", total_cart, KeyType.CART_ID); // different separator
                    loadTableFormatData(thread_id, B2WConstants.TABLENAME_CART_CUSTOMER,
                            config.CART_CUSTOMER_DATA_FILE, CART_CUSTOMER_TYPES, ",", total_cart_customer, KeyType.CART_ID);
                    loadTableFormatData(thread_id, B2WConstants.TABLENAME_CART_LINES,
                            config.CART_LINES_DATA_FILE, CART_LINES_TYPES, ",", total_cart_lines, KeyType.CART_ID);
                    loadTableFormatData(thread_id, B2WConstants.TABLENAME_CART_LINE_PRODUCTS,
                            config.CART_LINE_PRODUCTS_DATA_FILE, CART_LINE_PRODUCTS_TYPES, ";", total_cart_line_products, KeyType.CART_ID); // different separator
                    loadTableFormatData(thread_id, B2WConstants.TABLENAME_CART_LINE_PROMOTIONS,
                            config.CART_LINE_PROMOTIONS_DATA_FILE, CART_LINE_PROMOTIONS_TYPES, ",", total_cart_line_promotions, KeyType.CART_ID);
                    loadTableFormatData(thread_id, B2WConstants.TABLENAME_CART_LINE_PRODUCT_WARRANTIES,
                            config.CART_LINE_PRODUCT_WARRANTIES_DATA_FILE, CART_LINE_PRODUCT_WARRANTIES_TYPES, ";", total_cart_line_product_warranties, KeyType.CART_ID); // different separator
                    loadTableFormatData(thread_id, B2WConstants.TABLENAME_CART_LINE_PRODUCT_STORES,
                            config.CART_LINE_PRODUCT_STORES_DATA_FILE, CART_LINE_PRODUCT_STORES_TYPES, ",", total_cart_line_product_stores, KeyType.CART_ID);
                    loadTableFormatData(thread_id, B2WConstants.TABLENAME_CHECKOUT,
                            config.CHECKOUT_DATA_FILE, CHECKOUT_TYPES, ",", total_checkout, KeyType.CHECKOUT_ID);
                    loadTableFormatData(thread_id, B2WConstants.TABLENAME_CHECKOUT_PAYMENTS,
                            config.CHECKOUT_PAYMENTS_DATA_FILE, CHECKOUT_PAYMENTS_TYPES, ";", total_checkout_payments, KeyType.CHECKOUT_ID); // different separator
                    loadTableFormatData(thread_id, B2WConstants.TABLENAME_CHECKOUT_FREIGHT_DELIVERY_TIME,
                            config.CHECKOUT_FREIGHT_DELIVERY_TIME_DATA_FILE, CHECKOUT_FREIGHT_DELIVERY_TIME_TYPES, ",", total_checkout_freight_delivery_time, KeyType.CHECKOUT_ID);
                    loadTableFormatData(thread_id, B2WConstants.TABLENAME_CHECKOUT_STOCK_TRANSACTIONS,
                            config.CHECKOUT_STOCK_TRANSACTIONS_DATA_FILE, CHECKOUT_STOCK_TRANSACTIONS_TYPES, ",", total_checkout_stock_transactions, KeyType.CHECKOUT_ID);
                }
            });
        } // FOR
        ThreadUtil.runGlobalPool(runnables);
        
        LoaderSelector.closeAll();
        LOG.info("Load success!");      
    }



}
