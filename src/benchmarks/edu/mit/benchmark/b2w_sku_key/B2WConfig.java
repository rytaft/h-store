package edu.mit.benchmark.b2w_sku_key;

import java.util.Map;

import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ThreadUtil;

public class B2WConfig {
    private static final Logger LOG = Logger.getLogger(B2WConfig.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
     
    public Integer loadthreads = ThreadUtil.availableProcessors();
    public String operations_file = null;

    public String STK_INVENTORY_STOCK_DATA_FILE = null;
    public String STK_INVENTORY_STOCK_QUANTITY_DATA_FILE = null;
    public String STK_STOCK_TRANSACTION_DATA_FILE = null;
    public String CART_DATA_FILE = null;
    public String CART_CUSTOMER_DATA_FILE = null;
    public String CART_LINES_DATA_FILE = null;
    public String CART_LINE_PRODUCTS_DATA_FILE = null;
    public String CART_LINE_PROMOTIONS_DATA_FILE = null;
    public String CART_LINE_PRODUCT_WARRANTIES_DATA_FILE = null;
    public String CART_LINE_PRODUCT_STORES_DATA_FILE = null;
    public String CHECKOUT_DATA_FILE = null;
    public String CHECKOUT_PAYMENTS_DATA_FILE = null;
    public String CHECKOUT_FREIGHT_DELIVERY_TIME_DATA_FILE = null;
    public String CHECKOUT_STOCK_TRANSACTIONS_DATA_FILE = null;

    public Long speed_up = 200L;
    public Long sleep_time = 10L;
    public Long start_offset = 0L;

    
    public B2WConfig(Map<String, String> m_extraParams) {
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            // Multi-Threaded Loader
            if (key.equalsIgnoreCase("loadthreads")) {
                this.loadthreads  = Integer.valueOf(value);
            }
            else if (key.equalsIgnoreCase("operations_file")) {
                this.operations_file = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("speed_up")) {
                this.speed_up = Long.valueOf(value);
            }
            else if (key.equalsIgnoreCase("sleep_time")) {
                this.sleep_time = Long.valueOf(value);
            }
            else if (key.equalsIgnoreCase("start_offset")) {
                this.start_offset = Long.valueOf(value);
            }
            else if (key.equalsIgnoreCase("STK_INVENTORY_STOCK_DATA_FILE")) {
                this.STK_INVENTORY_STOCK_DATA_FILE = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("STK_INVENTORY_STOCK_QUANTITY_DATA_FILE")) {
                this.STK_INVENTORY_STOCK_QUANTITY_DATA_FILE = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("STK_STOCK_TRANSACTION_DATA_FILE")) {
                this.STK_STOCK_TRANSACTION_DATA_FILE = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("CART_DATA_FILE")) {
                this.CART_DATA_FILE = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("CART_CUSTOMER_DATA_FILE")) {
                this.CART_CUSTOMER_DATA_FILE = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("CART_LINES_DATA_FILE")) {
                this.CART_LINES_DATA_FILE = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("CART_LINE_PRODUCTS_DATA_FILE")) {
                this.CART_LINE_PRODUCTS_DATA_FILE = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("CART_LINE_PROMOTIONS_DATA_FILE")) {
                this.CART_LINE_PROMOTIONS_DATA_FILE = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("CART_LINE_PRODUCT_WARRANTIES_DATA_FILE")) {
                this.CART_LINE_PRODUCT_WARRANTIES_DATA_FILE = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("CART_LINE_PRODUCT_STORES_DATA_FILE")) {
                this.CART_LINE_PRODUCT_STORES_DATA_FILE = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("CHECKOUT_DATA_FILE")) {
                this.CHECKOUT_DATA_FILE = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("CHECKOUT_PAYMENTS_DATA_FILE")) {
                this.CHECKOUT_PAYMENTS_DATA_FILE = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("CHECKOUT_FREIGHT_DELIVERY_TIME_DATA_FILE")) {
                this.CHECKOUT_FREIGHT_DELIVERY_TIME_DATA_FILE = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("CHECKOUT_STOCK_TRANSACTIONS_DATA_FILE")) {
                this.CHECKOUT_STOCK_TRANSACTIONS_DATA_FILE = String.valueOf(value);
            }
        } // FOR
        
        LOG.info(this.toString());   
    }

    
    
    @Override
    public String toString() {
        return "B2WConfig [loadthreads=" + loadthreads
                + ", operations_file=" + operations_file 
                + ", STK_INVENTORY_STOCK_DATA_FILE=" + STK_INVENTORY_STOCK_DATA_FILE
                + ", STK_INVENTORY_STOCK_QUANTITY_DATA_FILE=" + STK_INVENTORY_STOCK_QUANTITY_DATA_FILE
                + ", STK_STOCK_TRANSACTION_DATA_FILE=" + STK_STOCK_TRANSACTION_DATA_FILE
                + ", CART_DATA_FILE=" + CART_DATA_FILE
                + ", CART_CUSTOMER_DATA_FILE=" + CART_CUSTOMER_DATA_FILE
                + ", CART_LINES_DATA_FILE=" + CART_LINES_DATA_FILE
                + ", CART_LINE_PRODUCTS_DATA_FILE=" + CART_LINE_PRODUCTS_DATA_FILE
                + ", CART_LINE_PROMOTIONS_DATA_FILE=" + CART_LINE_PROMOTIONS_DATA_FILE
                + ", CART_LINE_PRODUCT_WARRANTIES_DATA_FILE=" + CART_LINE_PRODUCT_WARRANTIES_DATA_FILE
                + ", CART_LINE_PRODUCT_STORES_DATA_FILE=" + CART_LINE_PRODUCT_STORES_DATA_FILE
                + ", CHECKOUT_DATA_FILE=" + CHECKOUT_DATA_FILE
                + ", CHECKOUT_PAYMENTS_DATA_FILE=" + CHECKOUT_PAYMENTS_DATA_FILE
                + ", CHECKOUT_FREIGHT_DELIVERY_TIME_DATA_FILE=" + CHECKOUT_FREIGHT_DELIVERY_TIME_DATA_FILE
                + ", CHECKOUT_STOCK_TRANSACTIONS_DATA_FILE=" + CHECKOUT_STOCK_TRANSACTIONS_DATA_FILE
                + ", speed_up=" + speed_up
                + ", sleep_time=" + sleep_time
                + ", start_offset=" + start_offset + "]";
    }

}
