package edu.mit.benchmark.b2w;

import java.util.Map;
import java.util.Random;

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
     
    public Random rand_gen;
    public Integer loadthreads = ThreadUtil.availableProcessors();
    public String cart_data_file = null;
    public String checkout_data_file = null;
    public String stock_transaction_data_file = null;
    public String stock_quantity_data_file = null;
    public String stock_inventory_data_file = null;
    public String operations_file = null;
    public Long speed_up = 1L;
    
    
    public B2WConfig(Map<String, String> m_extraParams) {
        this.rand_gen = new Random(); 
        
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            // Multi-Threaded Loader
            if (key.equalsIgnoreCase("loadthreads")) {
                this.loadthreads  = Integer.valueOf(value);
            }
            else if (key.equalsIgnoreCase("cart_data_file")) {
                this.cart_data_file = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("checkout_data_file")) {
                this.checkout_data_file = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("stock_transaction_data_file")) {
                this.stock_transaction_data_file = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("stock_quantity_data_file")) {
                this.stock_quantity_data_file = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("stock_inventory_data_file")) {
                this.stock_inventory_data_file = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("operations_file")) {
                this.operations_file = String.valueOf(value);
            }
            else if (key.equalsIgnoreCase("speed_up")) {
                this.speed_up = Long.valueOf(value);
            }
        } // FOR
            
    }

    
    
    @Override
    public String toString() {
        return "B2WConfig [loadthreads=" + loadthreads + ", cart_data_file=" + cart_data_file
                + ", checkout_data_file=" + checkout_data_file + ", stock_transaction_data_file=" + stock_transaction_data_file
                + ", stock_quantity_data_file=" + stock_quantity_data_file + ", stock_inventory_data_file=" + stock_inventory_data_file
                + ", operations_file=" + operations_file + "]";
    }

}
