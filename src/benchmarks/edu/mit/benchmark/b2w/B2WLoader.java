package edu.mit.benchmark.b2w;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
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
import org.voltdb.types.TimestampType;

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

    private TimestampType toTIMESTAMP(String time){
        SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
        Date date;
        try {
            date = format.parse(time);
        } catch (ParseException e) {
//            format = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
//            date = format.parse(time);
            LOG.error("invalid timestamp");
            date = new Date();
        }
        return new TimestampType(date);
    }
    
    private void loadCartData(Database catalog_db, String path) throws FileNotFoundException, JSONException {
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
            row_cart[param++] = cart.getString("status");
            row_cart[param++] = cart.getBoolean("autoMerge")?1:0;
            vt_cart.addRow(row_cart);

//            load table CART_CUSTOMER
            JSONObject customer = cart.getJSONObject("customer");
            Object row_customer[] = new Object[num_cols_customer];
            param = 0;
            row_customer[param++] = cart.getString("id");
            row_customer[param++] = customer.getString("id");
            row_customer[param++] = customer.getString("token");
            row_customer[param++] = customer.getBoolean("guest")?1:0;
            row_customer[param++] = customer.getBoolean("isGuest")?1:0;
            vt_customer.addRow(row_customer);

            JSONArray lines = cart.getJSONArray("lines");
            for (int i = 0; i < lines.length(); i++){
                JSONObject line = lines.getJSONObject(i);
                JSONObject product = line.getJSONObject("product");
                JSONObject store = product.getJSONObject("store");
                JSONObject stock_transaction = line.getJSONObject("stockTransaction");

//                load table CART_LINES
                Object row_lines[] = new Object[num_cols_lines];
                param = 0;
                row_lines[param++] = cart.getString("id");
                row_lines[param++] = line.getString("productSku");
                row_lines[param++] = Integer.parseInt(product.getString("sku"));
                row_lines[param++] = Integer.parseInt(product.getString("id"));
                row_lines[param++] = Integer.parseInt(store.getString("id"));
                row_lines[param++] = line.getDouble("unitSalesPrice");
                row_lines[param++] = line.getDouble("salesPrice");
                row_lines[param++] = line.getInt("quantity");
                row_lines[param++] = line.getInt("maxQuantity");
                row_lines[param++] = line.getString("maximumQuantityReason");
                row_lines[param++] = line.getString("type");
                row_lines[param++] = stock_transaction.getString("id");
                row_lines[param++] = stock_transaction.getString("requestedQuantity");
                row_lines[param++] = stock_transaction.getString("status");
                row_lines[param++] = stock_transaction.getString("stockType");
                row_lines[param++] = toTIMESTAMP(line.getString("insertDate"));
                vt_lines.addRow(row_lines);

//                load table CART_LINE_PRODUCTS
                Object row_products[] = new Object[num_cols_products];
                param = 0;
                row_products[param++] = cart.getString("id");
                row_products[param++] = line.getString("productSku");
                row_products[param++] = Integer.parseInt(product.getString("id"));
                row_products[param++] = Integer.parseInt(product.getString("sku"));
                row_products[param++] = product.getString("image");
                row_products[param++] = product.getString("name");
                row_products[param++] = product.getBoolean("isKit")?1:0;
                row_products[param++] = product.getDouble("price");
                row_products[param++] = product.getDouble("originalPrice");
                row_products[param++] = product.getBoolean("isLarge")?1:0;
                row_products[param++] = Integer.parseInt(product.getString("department"));
                row_products[param++] = Integer.parseInt(product.getString("line"));
                row_products[param++] = Integer.parseInt(product.getString("subClass"));
                row_products[param++] = product.getDouble("weight");
                row_products[param++] = Integer.parseInt(product.getString("class"));
                vt_products.addRow(row_products);

//                load table CART_LINE_PROMOTIONS
                JSONArray promotions = line.getJSONArray("promotions");
                for (int j = 0; j < promotions.length(); j++){
                    JSONObject promotion = promotions.getJSONObject(j);
                    Object row_promotions[] = new Object[num_cols_promotions];
                    param = 0;
                    row_promotions[param++] = cart.getString("id");
                    row_promotions[param++] = line.getString("productSku");
                    row_promotions[param++] = promotion.getString("name");
                    row_promotions[param++] = promotion.getString("category");
                    row_promotions[param++] = promotion.getDouble("sourceValue");
                    row_promotions[param++] = promotion.getString("type");
                    row_promotions[param++] = promotion.getBoolean("conditional")?1:0;
                    row_promotions[param++] = promotion.getDouble("discountValue");
                    vt_promotions.addRow(row_promotions);
                }


//                load table CART_LINE_PRODUCT_WARRANTIES
                JSONArray warranties = product.getJSONArray("warranties");
                for (int j = 0; j < warranties.length(); j++){
                    JSONObject warranty = warranties.getJSONObject(j);
                    Object row_warranties[] = new Object[num_cols_warranties];
                    param = 0;
                    row_warranties[param++] = cart.getString("id");
                    row_warranties[param++] = line.getString("productSku");
                    row_warranties[param++] = Integer.parseInt(warranty.getString("sku"));
//                    Yu Lu: I think the b2w-ddl.sql file is wrong at this point.
                    row_warranties[param++] = warranty.getString("productSku");
                    row_warranties[param++] = warranty.getString("description");
                    vt_warranties.addRow(row_warranties);
                }
                

//                load table CART_LINE_PRODUCT_STORES
                Object row_stores[] = new Object[num_cols_stores];
                param = 0;
                row_stores[param++] = cart.getString("id");
                row_stores[param++] = line.getString("productSku");
                row_stores[param++] = Integer.parseInt(store.getString("id"));
                row_stores[param++] = store.getString("name");
                row_stores[param++] = store.getString("image");
                row_stores[param++] = store.getString("deliveryType");
                vt_stores.addRow(row_stores);
            }
            
            
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
