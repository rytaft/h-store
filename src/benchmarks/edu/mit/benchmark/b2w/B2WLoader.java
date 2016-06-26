package edu.mit.benchmark.b2w;

import java.io.*;
import java.util.Calendar;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;

import com.oltpbenchmark.benchmarks.twitter.TwitterConstants;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.JSONUtil;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import edu.brown.api.BenchmarkComponent;
import edu.brown.api.Loader;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

import static com.google.gdata.util.ContentType.JSON;

public class B2WLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(B2WLoader.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }

    public final static int configCommitCount = 500;

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

//    Yu Lu : how to convert string into timestamp?
    private Object toTIMESTAMP(String time){
        return time;
    }
    
    protected void loadCartData(Database catalog_db, String path) throws FileNotFoundException, JSONException {
        JSONArray cart_lists = new JSONArray(FileUtil.readFile(new File(path)));
        
        Table catalog_tbl_cart = catalog_db.getTables().getIgnoreCase(B2WConstants.TABLENAME_CART);
        assert(catalog_tbl_cart != null);
        VoltTable vt_cart = CatalogUtil.getVoltTable(catalog_tbl_cart);
        int num_cols_cart = catalog_tbl_cart.getColumns().size();

        Table catalog_tbl_customer = catalog_db.getTables().getIgnoreCase(B2WConstants.TABLENAME_CART_CUSTOMER);
        assert(catalog_tbl_customer != null);
        VoltTable vt_customer = CatalogUtil.getVoltTable(catalog_tbl_customer);
        int num_cols_customer = catalog_tbl_customer.getColumns().size();

        Table catalog_tbl_lines = catalog_db.getTables().getIgnoreCase(B2WConstants.TABLENAME_CART_LINES);
        assert(catalog_tbl_lines != null);
        VoltTable vt_lines = CatalogUtil.getVoltTable(catalog_tbl_lines);
        int num_cols_lines = catalog_tbl_lines.getColumns().size();

        Table catalog_tbl_promotions = catalog_db.getTables().getIgnoreCase(B2WConstants.TABLENAME_CART_LINE_PROMOTIONS);
        assert(catalog_tbl_promotions != null);
        VoltTable vt_promotions = CatalogUtil.getVoltTable(catalog_tbl_promotions);
        int num_cols_promotions = catalog_tbl_promotions.getColumns().size();

        Table catalog_tbl_products = catalog_db.getTables().getIgnoreCase(B2WConstants.TABLENAME_CART_LINE_PRODUCTS);
        assert(catalog_tbl_products != null);
        VoltTable vt_products = CatalogUtil.getVoltTable(catalog_tbl_products);
        int num_cols_products = catalog_tbl_products.getColumns().size();

        Table catalog_tbl_warranties = catalog_db.getTables().getIgnoreCase(B2WConstants.TABLENAME_CART_LINE_PRODUCT_WARRANTIES);
        assert(catalog_tbl_warranties != null);
        VoltTable vt_warranties = CatalogUtil.getVoltTable(catalog_tbl_warranties);
        int num_cols_warranties = catalog_tbl_warranties.getColumns().size();

        Table catalog_tbl_stores = catalog_db.getTables().getIgnoreCase(B2WConstants.TABLENAME_CART_LINE_PRODUCT_STORES);
        assert(catalog_tbl_stores != null);
        VoltTable vt_stores = CatalogUtil.getVoltTable(catalog_tbl_stores);
        int num_cols_stores = catalog_tbl_stores.getColumns().size();

        int batchSize = 0;
        int total;

        for (total = 0; total < cart_lists.length(); total++) {
            JSONObject cart = cart_lists.getJSONObject(total);
            
//            load table CART
            Object row_cart[] = new Object[num_cols_cart];
            int param = 0;
            row_cart[param++] = cart.getString("id");
            row_cart[param++] = cart.getDouble("total");
            row_cart[param++] = cart.getString("salesChannel");
            row_cart[param++] = cart.getString("opn");
            row_cart[param++] = cart.getString("epar");
            row_cart[param++] = toTIMESTAMP(cart.getString("lastModified"));
//            Yu Lu : how to process status and autoMerge?
//            row_cart[param++] = cart.getString("status");            
//            row_cart[param++] = cart.getString("autoMerge");
            vt_cart.addRow(row_cart);

//            load table CART_CUSTOMER
            Object row_customer[] = new Object[num_cols_customer];
            param = 0;
            row_customer[param++] = cart.getString("id");
            vt_customer.addRow(row_customer);
//            load table CART_CUSTOMER
            Object row_customer[] = new Object[num_cols_customer];
            param = 0;
            row_customer[param++] = cart.getString("id");
            vt_customer.addRow(row_customer);

//            load table CART_CUSTOMER
            Object row_customer[] = new Object[num_cols_customer];
            param = 0;
            row_customer[param++] = cart.getString("id");
            vt_customer.addRow(row_customer);

//            load table CART_CUSTOMER
            Object row_customer[] = new Object[num_cols_customer];
            param = 0;
            row_customer[param++] = cart.getString("id");
            vt_customer.addRow(row_customer);

//            load table CART_CUSTOMER
            Object row_customer[] = new Object[num_cols_customer];
            param = 0;
            row_customer[param++] = cart.getString("id");
            vt_customer.addRow(row_customer);

//            load table CART_CUSTOMER
            Object row_customer[] = new Object[num_cols_customer];
            param = 0;
            row_customer[param++] = cart.getString("id");
            vt_customer.addRow(row_customer);
            
            
            batchSize++;
            
            if ((batchSize % configCommitCount) == 0) {
                this.loadVoltTable(catalog_tbl_cart.getName(), vt_cart);
                this.loadVoltTable(catalog_tbl_customer.getName(), vt_customer);
                this.loadVoltTable(catalog_tbl_lines.getName(), vt_lines);                
                this.loadVoltTable(catalog_tbl_products.getName(), vt_products);
                this.loadVoltTable(catalog_tbl_promotions.getName(), vt_promotions);
                this.loadVoltTable(catalog_tbl_stores.getName(), vt_stores);
                this.loadVoltTable(catalog_tbl_warranties.getName(), vt_warranties);
                vt_cart.clearRowData();
                vt_customer.clearRowData();
                vt_lines.clearRowData();
                vt_products.clearRowData();
                vt_promotions.clearRowData();
                vt_stores.clearRowData();
                vt_warranties.clearRowData();
                
                batchSize = 0;
                if (LOG.isDebugEnabled())
                    LOG.debug("Carts  % " + ((double)total/(double)cart_lists.length())*100);
                else if ((total % 100000) == 0) {
                    LOG.info("Carts  % " + ((double)total/(double)cart_lists.length())*100);
                }
            }
        }

        if (batchSize > 0) {
            this.loadVoltTable(catalog_tbl_cart.getName(), vt_cart);
            this.loadVoltTable(catalog_tbl_customer.getName(), vt_customer);
            this.loadVoltTable(catalog_tbl_lines.getName(), vt_lines);
            this.loadVoltTable(catalog_tbl_products.getName(), vt_products);
            this.loadVoltTable(catalog_tbl_promotions.getName(), vt_promotions);
            this.loadVoltTable(catalog_tbl_stores.getName(), vt_stores);
            this.loadVoltTable(catalog_tbl_warranties.getName(), vt_warranties);
            vt_cart.clearRowData();
            vt_customer.clearRowData();
            vt_lines.clearRowData();
            vt_products.clearRowData();
            vt_promotions.clearRowData();
            vt_stores.clearRowData();
            vt_warranties.clearRowData();
        }

        if (LOG.isDebugEnabled()) LOG.debug("[Follows Loaded] "+total);
        
        

    }

    @Override
    public void load() throws IOException {
        if (debug.val) {
            LOG.debug("Starting B2WLoader");
        }
        final CatalogContext catalogContext = this.getCatalogContext();
        try {
            this.loadCartData(catalogContext.database,"");
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }


 
}
