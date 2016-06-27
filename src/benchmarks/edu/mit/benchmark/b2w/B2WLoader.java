package edu.mit.benchmark.b2w;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;

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


    private final static int KEY_TYPE_INTEGER = 0;

    private final static int KEY_TYPE_VARCHAR = 1;

    private final static int KEY_TYPE_BIGINT = 2;

    private final static int KEY_TYPE_TINYINT = 3;

    private final static int KEY_TYPE_FLOAT = 4;

    private final static int KEY_TYPE_TIMESTAMP = 5;


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

    private Object getDataByType(JSONObject obj, int type, String key, String className) throws JSONException {
        if (obj == null || !obj.has(key)){
            if (debug.val) {
                LOG.debug(className + "is missing!!");
            }
            return null;
        }
        switch (type){
            case KEY_TYPE_INTEGER:
                return obj.getInt(key);
            case KEY_TYPE_VARCHAR:
                return obj.getString(key).toCharArray();
            case KEY_TYPE_BIGINT:
                return Integer.parseInt(obj.getString(key));
            case KEY_TYPE_TINYINT:
                return obj.getBoolean(key)?1:0;
            case KEY_TYPE_FLOAT:
                return obj.getDouble(key);
            case KEY_TYPE_TIMESTAMP:
                String time = obj.getString(key);
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
                Date date;
                try {
                    date = format.parse(time);
                } catch (ParseException e) {
                    format = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
                    try {
                        date = format.parse(time);
                    } catch (ParseException e1) {
                        LOG.error("invalid timestamp");
                        date = new Date();
                    }
                }
                return new TimestampType(date);
            default:
                return null;
        }
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
            row_cart[param++] = getDataByType(cart, KEY_TYPE_VARCHAR, "id", "cart " + total + ":cart[id]");
            row_cart[param++] = getDataByType(cart, KEY_TYPE_FLOAT, "total", "cart " + total + ":cart[total]");
            row_cart[param++] = getDataByType(cart, KEY_TYPE_VARCHAR, "salesChannel", "cart " + total + ":cart[salesChannel]");
            row_cart[param++] = getDataByType(cart, KEY_TYPE_VARCHAR, "opn", "cart " + total + ":cart[opn]");
            row_cart[param++] = getDataByType(cart, KEY_TYPE_VARCHAR, "epar", "cart " + total + ":cart[epar]");
            row_cart[param++] = getDataByType(cart, KEY_TYPE_TIMESTAMP, "lastModified", "cart " + total + ":cart[lastModified]");
            row_cart[param++] = getDataByType(cart, KEY_TYPE_VARCHAR, "status", "cart " + total + ":cart[status]");
            row_cart[param++] = getDataByType(cart, KEY_TYPE_TINYINT, "autoMerge", "cart " + total + ":cart[autoMerge]");
            vt_cart.addRow(row_cart);

//            load table CART_CUSTOMER
            JSONObject customer = cart.getJSONObject("customer");
            Object row_customer[] = new Object[num_cols_customer];
            param = 0;
            row_customer[param++] = getDataByType(cart, KEY_TYPE_VARCHAR, "id", "cart " + total + ":cart[id]");
            row_customer[param++] = getDataByType(customer, KEY_TYPE_VARCHAR, "id", "cart " + total + ":customer[id]");
            row_customer[param++] = getDataByType(customer, KEY_TYPE_VARCHAR, "token", "cart " + total + ":customer[token]");
            row_customer[param++] = getDataByType(customer, KEY_TYPE_TINYINT, "guest", "cart " + total + ":customer[guest]");
            row_customer[param++] = getDataByType(customer, KEY_TYPE_TINYINT, "isGuest", "cart " + total + ":customer[isGuest]");
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

                row_lines[param++] = getDataByType(cart, KEY_TYPE_VARCHAR, "id", "cart " + total + ":cart[id]");
                row_lines[param++] = getDataByType(line, KEY_TYPE_VARCHAR, "id", "cart " + total + ":line[id]");
                row_lines[param++] = getDataByType(product, KEY_TYPE_BIGINT, "sku", "cart " + total + ":product[sku]");
                row_lines[param++] = getDataByType(product, KEY_TYPE_BIGINT, "id", "cart " + total + ":product[id]");
                row_lines[param++] = getDataByType(store, KEY_TYPE_BIGINT, "id", "cart " + total + ":store[id]");
                row_lines[param++] = getDataByType(line, KEY_TYPE_FLOAT, "unitSalesPrice", "cart " + total + ":line[unitSalesPrice]");
                row_lines[param++] = getDataByType(line, KEY_TYPE_FLOAT, "salesPrice", "cart " + total + ":line[salesPrice]");
                row_lines[param++] = getDataByType(line, KEY_TYPE_INTEGER, "quantity", "cart " + total + ":line[quantity]");
                row_lines[param++] = getDataByType(line, KEY_TYPE_INTEGER, "maxQuantity", "cart " + total + ":line[maxQuantity]");
                row_lines[param++] = getDataByType(line, KEY_TYPE_VARCHAR, "maximumQuantityReason", "cart " + total + ":line[maximumQuantityReason]");
                row_lines[param++] = getDataByType(line, KEY_TYPE_VARCHAR, "type", "cart " + total + ":line[type]");
                row_lines[param++] = getDataByType(stock_transaction, KEY_TYPE_VARCHAR, "id", "cart " + total + ":stock_transaction[id]");
                row_lines[param++] = getDataByType(stock_transaction, KEY_TYPE_VARCHAR, "requestedQuantity", "cart " + total + ":stock_transaction[requestedQuantity]");
                row_lines[param++] = getDataByType(stock_transaction, KEY_TYPE_VARCHAR, "status", "cart " + total + ":stock_transaction[status]");
                row_lines[param++] = getDataByType(stock_transaction, KEY_TYPE_VARCHAR, "stockType", "cart " + total + ":stock_transaction[stockType]");
                row_lines[param++] = getDataByType(line, KEY_TYPE_TIMESTAMP, "insertDate", "cart " + total + ":line[insertDate]");
                vt_lines.addRow(row_lines);

//                load table CART_LINE_PRODUCTS
                Object row_products[] = new Object[num_cols_products];
                param = 0;
                row_products[param++] = getDataByType(cart, KEY_TYPE_VARCHAR, "id", "cart " + total + ":cart[id]");
                row_products[param++] = getDataByType(line, KEY_TYPE_VARCHAR, "id", "cart " + total + ":line[id]");
                row_products[param++] = getDataByType(product, KEY_TYPE_BIGINT, "id", "cart " + total + ":product[id]");
                row_products[param++] = getDataByType(product, KEY_TYPE_BIGINT, "sku", "cart " + total + ":product[sku]");
                row_products[param++] = getDataByType(product, KEY_TYPE_VARCHAR, "image", "cart " + total + ":product[image]");
                row_products[param++] = getDataByType(product, KEY_TYPE_VARCHAR, "name", "cart " + total + ":product[name]");
                row_products[param++] = getDataByType(product, KEY_TYPE_TINYINT, "isKit", "cart " + total + ":product[isKit]");
                row_products[param++] = getDataByType(product, KEY_TYPE_FLOAT, "price", "cart " + total + ":product[price]");
                row_products[param++] = getDataByType(product, KEY_TYPE_FLOAT, "originalPrice", "cart " + total + ":product[originalPrice]");
                row_products[param++] = getDataByType(product, KEY_TYPE_TINYINT, "isLarge", "cart " + total + ":product[isLarge]");
                row_products[param++] = getDataByType(product, KEY_TYPE_BIGINT, "department", "cart " + total + ":product[department]");
                row_products[param++] = getDataByType(product, KEY_TYPE_BIGINT, "line", "cart " + total + ":product[line]");
                row_products[param++] = getDataByType(product, KEY_TYPE_BIGINT, "subClass", "cart " + total + ":product[subClass]");
                row_products[param++] = getDataByType(product, KEY_TYPE_FLOAT, "weight", "cart " + total + ":product[weight]");
                row_products[param++] = getDataByType(product, KEY_TYPE_BIGINT, "class", "cart " + total + ":product[class]");
                vt_products.addRow(row_products);

//                load table CART_LINE_PROMOTIONS
                JSONArray promotions = line.getJSONArray("promotions");
                for (int j = 0; j < promotions.length(); j++){
                    JSONObject promotion = promotions.getJSONObject(j);
                    Object row_promotions[] = new Object[num_cols_promotions];
                    param = 0;
                    row_promotions[param++] = getDataByType(cart, KEY_TYPE_VARCHAR, "id", "cart " + total + ":cart[id]");
                    row_promotions[param++] = getDataByType(line, KEY_TYPE_VARCHAR, "id", "cart " + total + ":line[id]");
                    row_promotions[param++] = getDataByType(promotion, KEY_TYPE_VARCHAR, "name", "cart " + total + ":promotion[name]");
                    row_promotions[param++] = getDataByType(promotion, KEY_TYPE_VARCHAR, "category", "cart " + total + ":promotion[category]");
                    row_promotions[param++] = getDataByType(promotion, KEY_TYPE_FLOAT, "sourceValue", "cart " + total + ":promotion[sourceValue]");
                    row_promotions[param++] = getDataByType(promotion, KEY_TYPE_VARCHAR, "type", "cart " + total + ":promotion[type]");
                    row_promotions[param++] = getDataByType(promotion, KEY_TYPE_TINYINT, "conditional", "cart " + total + ":promotion[conditional]");
                    row_promotions[param++] = getDataByType(promotion, KEY_TYPE_FLOAT, "discountValue", "cart " + total + ":promotion[discountValue]");
                    vt_promotions.addRow(row_promotions);
                }


//                load table CART_LINE_PRODUCT_WARRANTIES
                JSONArray warranties = product.getJSONArray("warranties");
                for (int j = 0; j < warranties.length(); j++){
                    JSONObject warranty = warranties.getJSONObject(j);
                    Object row_warranties[] = new Object[num_cols_warranties];
                    param = 0;
                    row_warranties[param++] = getDataByType(cart, KEY_TYPE_VARCHAR, "id", "cart " + total + ":cart[id]");
                    row_warranties[param++] = getDataByType(line, KEY_TYPE_VARCHAR, "id", "cart " + total + ":line[id]");
                    row_warranties[param++] = getDataByType(warranty, KEY_TYPE_BIGINT, "sku", "cart " + total + ":warranty[sku]");
                    row_warranties[param++] = getDataByType(warranty, KEY_TYPE_VARCHAR, "productSku", "cart " + total + ":warranty[productSku]");
                    row_warranties[param++] = getDataByType(warranty, KEY_TYPE_VARCHAR, "description", "cart " + total + ":warranty[description]");
                    vt_warranties.addRow(row_warranties);
                }


//                load table CART_LINE_PRODUCT_STORES
                Object row_stores[] = new Object[num_cols_stores];
                param = 0;
                row_stores[param++] = getDataByType(cart, KEY_TYPE_VARCHAR, "id", "cart " + total + ":cart[id]");
                row_stores[param++] = getDataByType(line, KEY_TYPE_VARCHAR, "id", "cart " + total + ":line[id]");
                row_stores[param++] = getDataByType(store, KEY_TYPE_BIGINT, "id", "cart " + total + ":store[id]");
                row_stores[param++] = getDataByType(store, KEY_TYPE_VARCHAR, "name", "cart " + total + ":store[name]");
                row_stores[param++] = getDataByType(store, KEY_TYPE_VARCHAR, "image", "cart " + total + ":store[image]");
                row_stores[param++] = getDataByType(store, KEY_TYPE_VARCHAR, "deliveryType", "cart " + total + ":store[deliveryType]");
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

        if (LOG.isDebugEnabled()) LOG.debug("[Carts Loaded] "+total);
    }

    private void loadCheckOutData(Database catalog_db, String path) throws FileNotFoundException, JSONException {
        JSONObject checkout = new JSONObject(FileUtil.readFile(new File(path)));

        Table catalog_tbl_checkout = catalog_db.getTables().getIgnoreCase(B2WConstants.TABLENAME_CHECKOUT);
        assert(catalog_tbl_checkout != null);
        VoltTable vt_checkout = CatalogUtil.getVoltTable(catalog_tbl_checkout);
        int num_cols_checkout = catalog_tbl_checkout.getColumns().size();

        Table catalog_tbl_freight = catalog_db.getTables().getIgnoreCase(B2WConstants.TABLENAME_CHECKOUT_FREIGHT_DELIVERY_TIME);
        assert(catalog_tbl_freight != null);
        VoltTable vt_freight = CatalogUtil.getVoltTable(catalog_tbl_freight);
        int num_cols_freight = catalog_tbl_freight.getColumns().size();
        
        Table catalog_tbl_payments = catalog_db.getTables().getIgnoreCase(B2WConstants.TABLENAME_CHECKOUT_PAYMENTS);
        assert(catalog_tbl_payments != null);
        VoltTable vt_payments = CatalogUtil.getVoltTable(catalog_tbl_payments);
        int num_cols_payments = catalog_tbl_payments.getColumns().size();
        
        Table catalog_tbl_stock = catalog_db.getTables().getIgnoreCase(B2WConstants.TABLENAME_CHECKOUT_STOCK_TRANSACTIONS);
        assert(catalog_tbl_stock != null);
        VoltTable vt_stock = CatalogUtil.getVoltTable(catalog_tbl_stock);
        int num_cols_stock = catalog_tbl_stock.getColumns().size();

        //            load table CART
        Object row_checkout[] = new Object[num_cols_checkout];
        int param = 0;
        row_checkout[param++] = getDataByType(checkout, KEY_TYPE_VARCHAR, "id", path + ":checkout[id]");
        row_checkout[param++] = getDataByType(checkout, KEY_TYPE_FLOAT, "total", path + ":checkout[total]");
        vt_checkout.addRow(row_checkout);


        this.loadVoltTable(catalog_tbl_checkout.getName(), vt_checkout);
        this.loadVoltTable(catalog_tbl_freight.getName(), vt_freight);
        this.loadVoltTable(catalog_tbl_payments.getName(), vt_payments);
        this.loadVoltTable(catalog_tbl_stock.getName(), vt_stock);
        vt_checkout.clearRowData();
        vt_freight.clearRowData();
        vt_payments.clearRowData();
        vt_stock.clearRowData();
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
            LOG.error("JSON load failed while loadCartData");
            e.printStackTrace();
        }
    }


 
}
