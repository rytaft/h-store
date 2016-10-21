package edu.mit.benchmark.b2w;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import edu.brown.catalog.CatalogUtil;
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
import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;

@SuppressWarnings("ALL")
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

    private final static int KEY_TYPE_OBJECT = 6;

    private final static int KEY_TYPE_ARRAY = 7;

    private final static byte TINY_INT_TRUE = 1;

    private final static byte TINY_INT_FALSE = 0;

    private final static int[] INVENTORY_STOCK_TYPES = {
            KEY_TYPE_VARCHAR,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_INTEGER,
            KEY_TYPE_INTEGER,
            KEY_TYPE_INTEGER,
            KEY_TYPE_VARCHAR,
            KEY_TYPE_INTEGER};

    private final static int[] STOCK_QUANTITY_TYPES = {
            KEY_TYPE_VARCHAR,
            KEY_TYPE_INTEGER,
            KEY_TYPE_INTEGER,
            KEY_TYPE_INTEGER};

    private final static int[] STOCK_TRANSACTION_TYPES = {
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
            KEY_TYPE_INTEGER};

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
     * This function is used to get the item from a JSONObject by key and translate it into specific type
     * which could be accepted by VoltType.
     * @param obj the JSONObject
     * @param type the target type we want to get.
     * @param key the key of the item, we can put the key into JSONObject's get function to get the value we want.
     * @param dataName the name of the data, it is only used for debug. When the item is missing, it will send a
     *                 debug message dataName + "is missing!".
     * @return return the target object we want, if the value is null or the type is invalid, then return null.
     */
    private Object getDataByType(JSONObject obj, int type, String key, String dataName)
            throws JSONException {
        if (obj == null || !obj.has(key)){
            if (debug.val) {
                LOG.debug(dataName + "is missing!!");
            }
            if (type == KEY_TYPE_ARRAY)
                return new JSONArray();
            else
                return null;
        }
        switch (type){
            case KEY_TYPE_INTEGER:
                return obj.getInt(key);
            case KEY_TYPE_VARCHAR:
                return obj.getString(key);
            case KEY_TYPE_BIGINT:
                return Long.parseLong(obj.getString(key));
            case KEY_TYPE_TINYINT:
                return obj.getBoolean(key)?TINY_INT_TRUE:TINY_INT_FALSE;
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
            case KEY_TYPE_OBJECT:
                return obj.getJSONObject(key);
            case KEY_TYPE_ARRAY:
                return obj.getJSONArray(key);

            default:
                return null;
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
        if (value.equals("null") || value.equals("<null>"))
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
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
                Date date;
                try {
                    date = format.parse(value);
                } catch (ParseException e) {
                    LOG.error("invalid timestamp");
                    date = new Date();
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


    public static Integer hashPartition(Object o){
        if (o == null)
            return null;
        else
            return o.hashCode();
    }



    private void loadCartData(Database catalog_db, String path) throws IOException, JSONException {
        BufferedReader in = new BufferedReader(new InputStreamReader(
                new FileInputStream(path)));

        Table catalog_tbl_cart = catalog_db.getTables().getIgnoreCase(
                B2WConstants.TABLENAME_CART);
        assert(catalog_tbl_cart != null);
        VoltTable vt_cart = CatalogUtil.getVoltTable(catalog_tbl_cart);
        int num_cols_cart = catalog_tbl_cart.getColumns().size();

        Table catalog_tbl_customer = catalog_db.getTables().getIgnoreCase(
                B2WConstants.TABLENAME_CART_CUSTOMER);
        assert(catalog_tbl_customer != null);
        VoltTable vt_customer = CatalogUtil.getVoltTable(catalog_tbl_customer);
        int num_cols_customer = catalog_tbl_customer.getColumns().size();

        Table catalog_tbl_lines = catalog_db.getTables().getIgnoreCase(
                B2WConstants.TABLENAME_CART_LINES);
        assert(catalog_tbl_lines != null);
        VoltTable vt_lines = CatalogUtil.getVoltTable(catalog_tbl_lines);
        int num_cols_lines = catalog_tbl_lines.getColumns().size();

        Table catalog_tbl_promotions = catalog_db.getTables().getIgnoreCase(
                B2WConstants.TABLENAME_CART_LINE_PROMOTIONS);
        assert(catalog_tbl_promotions != null);
        VoltTable vt_promotions = CatalogUtil.getVoltTable(catalog_tbl_promotions);
        int num_cols_promotions = catalog_tbl_promotions.getColumns().size();

        Table catalog_tbl_products = catalog_db.getTables().getIgnoreCase(
                B2WConstants.TABLENAME_CART_LINE_PRODUCTS);
        assert(catalog_tbl_products != null);
        VoltTable vt_products = CatalogUtil.getVoltTable(catalog_tbl_products);
        int num_cols_products = catalog_tbl_products.getColumns().size();

        Table catalog_tbl_warranties = catalog_db.getTables().getIgnoreCase(
                B2WConstants.TABLENAME_CART_LINE_PRODUCT_WARRANTIES);
        assert(catalog_tbl_warranties != null);
        VoltTable vt_warranties = CatalogUtil.getVoltTable(catalog_tbl_warranties);
        int num_cols_warranties = catalog_tbl_warranties.getColumns().size();

        Table catalog_tbl_stores = catalog_db.getTables().getIgnoreCase(
                B2WConstants.TABLENAME_CART_LINE_PRODUCT_STORES);
        assert(catalog_tbl_stores != null);
        VoltTable vt_stores = CatalogUtil.getVoltTable(catalog_tbl_stores);
        int num_cols_stores = catalog_tbl_stores.getColumns().size();

        int batchSize = 0;
        int total = 0;
        while (true){
            String cart_line = in.readLine();
            if (cart_line == null || cart_line.isEmpty())
                break;
            total++;
            JSONObject cart = new JSONObject(cart_line);
//            load table CART
            Object row_cart[] = new Object[num_cols_cart];
            int param = 0;
            row_cart[param++] = hashPartition(getDataByType(cart, KEY_TYPE_VARCHAR,
                    "id", "cart " + total + ":cart[partition_key]"));
            row_cart[param++] = getDataByType(cart, KEY_TYPE_VARCHAR,
                    "id", "cart " + total + ":cart[id]");
            row_cart[param++] = getDataByType(cart, KEY_TYPE_FLOAT,
                    "total", "cart " + total + ":cart[total]");
            row_cart[param++] = getDataByType(cart, KEY_TYPE_VARCHAR,
                    "salesChannel", "cart " + total + ":cart[salesChannel]");
            row_cart[param++] = getDataByType(cart, KEY_TYPE_VARCHAR,
                    "opn", "cart " + total + ":cart[opn]");
            row_cart[param++] = getDataByType(cart, KEY_TYPE_VARCHAR,
                    "epar", "cart " + total + ":cart[epar]");
            row_cart[param++] = getDataByType(cart, KEY_TYPE_TIMESTAMP,
                    "lastModified", "cart " + total + ":cart[lastModified]");
            row_cart[param++] = getDataByType(cart, KEY_TYPE_VARCHAR,
                    "status", "cart " + total + ":cart[status]");
            row_cart[param++] = getDataByType(cart, KEY_TYPE_TINYINT,
                    "autoMerge", "cart " + total + ":cart[autoMerge]");
            vt_cart.addRow(row_cart);

//            load table CART_CUSTOMER
            JSONObject customer = (JSONObject)getDataByType(cart, KEY_TYPE_OBJECT,
                    "customer", "cart " + total + ":cart[customer]");
            if (customer != null){
                Object row_customer[] = new Object[num_cols_customer];
                param = 0;
                row_customer[param++] = hashPartition(getDataByType(cart, KEY_TYPE_VARCHAR,
                        "id", "cart " + total + ":cart[partition_key]"));
                row_customer[param++] = getDataByType(cart, KEY_TYPE_VARCHAR,
                        "id", "cart " + total + ":cart[id]");
                row_customer[param++] = getDataByType(customer, KEY_TYPE_VARCHAR,
                        "id", "cart " + total + ":customer[id]");
                row_customer[param++] = getDataByType(customer, KEY_TYPE_VARCHAR,
                        "token", "cart " + total + ":customer[token]");
                row_customer[param++] = getDataByType(customer, KEY_TYPE_TINYINT,
                        "guest", "cart " + total + ":customer[guest]");
                row_customer[param++] = getDataByType(customer, KEY_TYPE_TINYINT,
                        "isGuest", "cart " + total + ":customer[isGuest]");
                vt_customer.addRow(row_customer);
            }


            JSONArray lines = (JSONArray)getDataByType(cart, KEY_TYPE_ARRAY, "lines", "cart " + total + ":cart[lines]");
            for (int i = 0; i < lines.length(); i++){
                JSONObject line = lines.getJSONObject(i);
                JSONObject product = (JSONObject)getDataByType(line, KEY_TYPE_OBJECT,
                        "product", "cart " + total + ":line[product]");
                JSONObject store = (JSONObject)getDataByType(product, KEY_TYPE_OBJECT,
                        "store", "cart " + total + ":product[store]");
                JSONObject stock_transaction = (JSONObject)getDataByType(line, KEY_TYPE_OBJECT,
                        "stockTransaction", "cart " + total + ":line[stockTransaction]");

//                load table CART_LINES
                Object row_lines[] = new Object[num_cols_lines];
                param = 0;
                row_lines[param++] = hashPartition(getDataByType(cart, KEY_TYPE_VARCHAR,
                        "id", "cart " + total + ":cart[partition_key]"));
                row_lines[param++] = getDataByType(cart, KEY_TYPE_VARCHAR,
                        "id", "cart " + total + ":cart[id]");
                row_lines[param++] = getDataByType(line, KEY_TYPE_VARCHAR,
                        "id", "cart " + total + ":line[id]");
                row_lines[param++] = getDataByType(product, KEY_TYPE_VARCHAR,
                        "sku", "cart " + total + ":product[sku]");
                row_lines[param++] = getDataByType(product, KEY_TYPE_BIGINT,
                        "id", "cart " + total + ":product[id]");
                row_lines[param++] = getDataByType(store, KEY_TYPE_VARCHAR,
                        "id", "cart " + total + ":store[id]");
                row_lines[param++] = getDataByType(line, KEY_TYPE_FLOAT,
                        "unitSalesPrice", "cart " + total + ":line[unitSalesPrice]");
                row_lines[param++] = getDataByType(line, KEY_TYPE_FLOAT,
                        "salesPrice", "cart " + total + ":line[salesPrice]");
                row_lines[param++] = getDataByType(line, KEY_TYPE_INTEGER,
                        "quantity", "cart " + total + ":line[quantity]");
                row_lines[param++] = getDataByType(line, KEY_TYPE_INTEGER,
                        "maxQuantity", "cart " + total + ":line[maxQuantity]");
                row_lines[param++] = getDataByType(line, KEY_TYPE_VARCHAR,
                        "maximumQuantityReason", "cart " + total + ":line[maximumQuantityReason]");
                row_lines[param++] = getDataByType(line, KEY_TYPE_VARCHAR,
                        "type", "cart " + total + ":line[type]");
                row_lines[param++] = getDataByType(stock_transaction, KEY_TYPE_VARCHAR,
                        "id", "cart " + total + ":stock_transaction[id]");
                row_lines[param++] = getDataByType(stock_transaction, KEY_TYPE_INTEGER,
                        "requestedQuantity", "cart " + total + ":stock_transaction[requestedQuantity]");
                row_lines[param++] = getDataByType(stock_transaction, KEY_TYPE_VARCHAR,
                        "status", "cart " + total + ":stock_transaction[status]");
                row_lines[param++] = getDataByType(stock_transaction, KEY_TYPE_VARCHAR,
                        "stockType", "cart " + total + ":stock_transaction[stockType]");
                row_lines[param++] = getDataByType(line, KEY_TYPE_TIMESTAMP,
                        "insertDate", "cart " + total + ":line[insertDate]");
                vt_lines.addRow(row_lines);

//                load table CART_LINE_PRODUCTS
                if (product != null){
                    Object row_products[] = new Object[num_cols_products];
                    param = 0;
                    row_products[param++] = hashPartition(getDataByType(cart, KEY_TYPE_VARCHAR,
                            "id", "cart " + total + ":cart[partition_key]"));
                    row_products[param++] = getDataByType(cart, KEY_TYPE_VARCHAR,
                            "id", "cart " + total + ":cart[id]");
                    row_products[param++] = getDataByType(line, KEY_TYPE_VARCHAR,
                            "id", "cart " + total + ":line[id]");
                    row_products[param++] = getDataByType(product, KEY_TYPE_BIGINT,
                            "id", "cart " + total + ":product[id]");
                    row_products[param++] = getDataByType(product, KEY_TYPE_VARCHAR,
                            "sku", "cart " + total + ":product[sku]");
                    row_products[param++] = getDataByType(product, KEY_TYPE_VARCHAR,
                            "image", "cart " + total + ":product[image]");
                    row_products[param++] = getDataByType(product, KEY_TYPE_VARCHAR,
                            "name", "cart " + total + ":product[name]");
                    row_products[param++] = getDataByType(product, KEY_TYPE_TINYINT,
                            "isKit", "cart " + total + ":product[isKit]");
                    row_products[param++] = getDataByType(product, KEY_TYPE_FLOAT,
                            "price", "cart " + total + ":product[price]");
                    row_products[param++] = getDataByType(product, KEY_TYPE_FLOAT,
                            "originalPrice", "cart " + total + ":product[originalPrice]");
                    row_products[param++] = getDataByType(product, KEY_TYPE_TINYINT,
                            "isLarge", "cart " + total + ":product[isLarge]");
                    row_products[param++] = getDataByType(product, KEY_TYPE_BIGINT,
                            "department", "cart " + total + ":product[department]");
                    row_products[param++] = getDataByType(product, KEY_TYPE_BIGINT,
                            "line", "cart " + total + ":product[line]");
                    row_products[param++] = getDataByType(product, KEY_TYPE_BIGINT,
                            "subClass", "cart " + total + ":product[subClass]");
                    row_products[param++] = getDataByType(product, KEY_TYPE_FLOAT,
                            "weight", "cart " + total + ":product[weight]");
                    row_products[param++] = getDataByType(product, KEY_TYPE_BIGINT,
                            "class", "cart " + total + ":product[class]");
                    vt_products.addRow(row_products);
                }

//                load table CART_LINE_PROMOTIONS
                JSONArray promotions = (JSONArray)getDataByType(line, KEY_TYPE_ARRAY, "promotions", "cart " + total + ":line[promotions]");
                for (int j = 0; j < promotions.length(); j++){
                    JSONObject promotion = promotions.getJSONObject(j);
                    Object row_promotions[] = new Object[num_cols_promotions];
                    param = 0;
                    row_promotions[param++] = hashPartition(getDataByType(cart, KEY_TYPE_VARCHAR,
                            "id", "cart " + total + ":cart[partition_key]"));
                    row_promotions[param++] = getDataByType(cart, KEY_TYPE_VARCHAR,
                            "id", "cart " + total + ":cart[id]");
                    row_promotions[param++] = getDataByType(line, KEY_TYPE_VARCHAR,
                            "id", "cart " + total + ":line[id]");
                    row_promotions[param++] = getDataByType(promotion, KEY_TYPE_VARCHAR,
                            "name", "cart " + total + ":promotion[name]");
                    row_promotions[param++] = getDataByType(promotion, KEY_TYPE_VARCHAR,
                            "category", "cart " + total + ":promotion[category]");
                    row_promotions[param++] = getDataByType(promotion, KEY_TYPE_FLOAT,
                            "sourceValue", "cart " + total + ":promotion[sourceValue]");
                    row_promotions[param++] = getDataByType(promotion, KEY_TYPE_VARCHAR,
                            "type", "cart " + total + ":promotion[type]");
                    row_promotions[param++] = getDataByType(promotion, KEY_TYPE_TINYINT,
                            "conditional", "cart " + total + ":promotion[conditional]");
                    row_promotions[param++] = getDataByType(promotion, KEY_TYPE_FLOAT,
                            "discountValue", "cart " + total + ":promotion[discountValue]");
                    vt_promotions.addRow(row_promotions);
                }


//                load table CART_LINE_PRODUCT_WARRANTIES
                JSONArray warranties = (JSONArray)getDataByType(product, KEY_TYPE_ARRAY,
                        "warranties", "cart " + total + ":product[warranties]");
                for (int j = 0; j < warranties.length(); j++){
                    JSONObject warranty = warranties.getJSONObject(j);
                    Object row_warranties[] = new Object[num_cols_warranties];
                    param = 0;
                    row_warranties[param++] = hashPartition(getDataByType(cart, KEY_TYPE_VARCHAR,
                            "id", "cart " + total + ":cart[partition_key]"));
                    row_warranties[param++] = getDataByType(cart, KEY_TYPE_VARCHAR,
                            "id", "cart " + total + ":cart[id]");
                    row_warranties[param++] = getDataByType(line, KEY_TYPE_VARCHAR,
                            "id", "cart " + total + ":line[id]");
                    row_warranties[param++] = getDataByType(warranty, KEY_TYPE_VARCHAR,
                            "sku", "cart " + total + ":warranty[sku]");
                    row_warranties[param++] = getDataByType(warranty, KEY_TYPE_VARCHAR,
                            "productSku", "cart " + total + ":warranty[productSku]");
                    row_warranties[param++] = getDataByType(warranty, KEY_TYPE_VARCHAR,
                            "description", "cart " + total + ":warranty[description]");
                    vt_warranties.addRow(row_warranties);
                }


//                load table CART_LINE_PRODUCT_STORES
                if (store != null){
                    Object row_stores[] = new Object[num_cols_stores];
                    param = 0;
                    row_stores[param++] = hashPartition(getDataByType(cart, KEY_TYPE_VARCHAR,
                            "id", "cart " + total + ":cart[partition_key]"));
                    row_stores[param++] = getDataByType(cart, KEY_TYPE_VARCHAR,
                            "id", "cart " + total + ":cart[id]");
                    row_stores[param++] = getDataByType(line, KEY_TYPE_VARCHAR,
                            "id", "cart " + total + ":line[id]");
                    row_stores[param++] = getDataByType(store, KEY_TYPE_VARCHAR,
                            "id", "cart " + total + ":store[id]");
                    row_stores[param++] = getDataByType(store, KEY_TYPE_VARCHAR,
                            "name", "cart " + total + ":store[name]");
                    row_stores[param++] = getDataByType(store, KEY_TYPE_VARCHAR,
                            "image", "cart " + total + ":store[image]");
                    row_stores[param++] = getDataByType(store, KEY_TYPE_VARCHAR,
                            "deliveryType", "cart " + total + ":store[deliveryType]");
                    vt_stores.addRow(row_stores);
                }
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
                    LOG.debug("Carts  % " + total);
                else if ((total % 100000) == 0) {
                    LOG.info("Carts  % " + total);
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
        in.close();
    }

    private void loadCheckOutData(Database catalog_db, String path)
            throws IOException, JSONException {
        BufferedReader in = new BufferedReader(new InputStreamReader(
                new FileInputStream(path)));


        Table catalog_tbl_checkout = catalog_db.getTables().getIgnoreCase(
                B2WConstants.TABLENAME_CHECKOUT);
        assert(catalog_tbl_checkout != null);
        VoltTable vt_checkout = CatalogUtil.getVoltTable(catalog_tbl_checkout);
        int num_cols_checkout = catalog_tbl_checkout.getColumns().size();

        Table catalog_tbl_freight = catalog_db.getTables().getIgnoreCase(
                B2WConstants.TABLENAME_CHECKOUT_FREIGHT_DELIVERY_TIME);
        assert(catalog_tbl_freight != null);
        VoltTable vt_freight = CatalogUtil.getVoltTable(catalog_tbl_freight);
        int num_cols_freight = catalog_tbl_freight.getColumns().size();
        
        Table catalog_tbl_payments = catalog_db.getTables().getIgnoreCase(
                B2WConstants.TABLENAME_CHECKOUT_PAYMENTS);
        assert(catalog_tbl_payments != null);
        VoltTable vt_payments = CatalogUtil.getVoltTable(catalog_tbl_payments);
        int num_cols_payments = catalog_tbl_payments.getColumns().size();
        
        Table catalog_tbl_stock = catalog_db.getTables().getIgnoreCase(
                B2WConstants.TABLENAME_CHECKOUT_STOCK_TRANSACTIONS);
        assert(catalog_tbl_stock != null);
        VoltTable vt_stock = CatalogUtil.getVoltTable(catalog_tbl_stock);
        int num_cols_stock = catalog_tbl_stock.getColumns().size();

        int batchSize = 0;
        int total = 0;
        while (true) {
            String checkout_line = in.readLine();
            if (checkout_line == null || checkout_line.isEmpty())
                break;
            JSONObject checkout;
            try{
                checkout = new JSONObject(checkout_line).getJSONObject("checkout");
            } catch (Exception e){
                LOG.error("Can't find checkout in checkout identity :" + checkout_line);
                continue;
            }
            total++;
//            load table CHECKOUT
            JSONObject freight = (JSONObject)getDataByType(checkout, KEY_TYPE_OBJECT,
                    "freight", path + ":checkout[freight]");
            Object row_checkout[] = new Object[num_cols_checkout];
            int param = 0;
            row_checkout[param++] = hashPartition(getDataByType(checkout, KEY_TYPE_VARCHAR,
                    "id", path + ":checkout[partition_key]"));
            row_checkout[param++] = getDataByType(checkout, KEY_TYPE_VARCHAR,
                    "id", path + ":checkout[id]");
            row_checkout[param++] = getDataByType(checkout, KEY_TYPE_VARCHAR,
                    "cartId", path + ":checkout[cartId]");
            row_checkout[param++] = getDataByType(checkout, KEY_TYPE_VARCHAR,
                    "deliveryAddressId", path + ":checkout[deliveryAddressId]");
            row_checkout[param++] = getDataByType(checkout, KEY_TYPE_VARCHAR,
                    "billingAddressId", path + ":checkout[billingAddressId]");
            row_checkout[param++] = getDataByType(checkout, KEY_TYPE_FLOAT,
                    "amountDue", path + ":checkout[amountDue]");
            row_checkout[param++] = getDataByType(checkout, KEY_TYPE_FLOAT,
                    "total", path + ":checkout[total]");
            row_checkout[param++] = getDataByType(freight, KEY_TYPE_VARCHAR,
                    "contract", path + ":freight[contract]");
            row_checkout[param++] = getDataByType(freight, KEY_TYPE_FLOAT,
                    "price", path + ":freight[price]");
            row_checkout[param++] = getDataByType(freight, KEY_TYPE_VARCHAR,
                    "status", path + ":freight[status]");

            vt_checkout.addRow(row_checkout);

//            load table CHECKOUT_PAYMENTS
            JSONArray payments = (JSONArray) getDataByType(checkout, KEY_TYPE_ARRAY, "payments", path + ":checkout[payments]");
            for (int i = 0; i < payments.length(); i++){
                JSONObject payment = payments.getJSONObject(i);

                Object row_payments[] = new Object[num_cols_payments];
                param = 0;
                row_payments[param++] = hashPartition(getDataByType(checkout, KEY_TYPE_VARCHAR,
                        "id", path + ":checkout[partition_key]"));
                row_payments[param++] = getDataByType(checkout, KEY_TYPE_VARCHAR,
                        "id", path + ":checkout[id]");
                row_payments[param++] = getDataByType(payment, KEY_TYPE_VARCHAR,
                        "paymentOptionId", path + ":payment[paymentOptionId]");
                row_payments[param++] = getDataByType(payment, KEY_TYPE_VARCHAR,
                        "paymentOptionType", path + ":payment[paymentOptionType]");
                row_payments[param++] = getDataByType(payment, KEY_TYPE_INTEGER,
                        "dueDays", path + ":payment[dueDays]");
                row_payments[param++] = getDataByType(payment, KEY_TYPE_FLOAT,
                        "amount", path + ":payment[amount]");
                row_payments[param++] = getDataByType(payment, KEY_TYPE_INTEGER,
                        "installmentQuantity", path + ":payment[installmentQuantity]");
                row_payments[param++] = getDataByType(payment, KEY_TYPE_FLOAT,
                        "interestAmount", path + ":payment[interestAmount]");
                row_payments[param++] = getDataByType(payment, KEY_TYPE_INTEGER,
                        "interestRate", path + ":payment[interestRate]");
                row_payments[param++] = getDataByType(payment, KEY_TYPE_INTEGER,
                        "annualCET", path + ":payment[annualCET]");
                row_payments[param++] = getDataByType(payment, KEY_TYPE_VARCHAR,
                        "number", path + ":payment[number]");
                row_payments[param++] = getDataByType(payment, KEY_TYPE_VARCHAR,
                        "criptoNumber", path + ":payment[criptoNumber]");
                row_payments[param++] = getDataByType(payment, KEY_TYPE_VARCHAR,
                        "holdersName", path + ":payment[holdersName]");
                row_payments[param++] = getDataByType(payment, KEY_TYPE_VARCHAR,
                        "securityCode", path + ":payment[securityCode]");
                row_payments[param++] = getDataByType(payment, KEY_TYPE_VARCHAR,
                        "expirationDate", path + ":payment[expirationDate]");

                vt_payments.addRow(row_payments);
            }


//            load table CHECKOUT_FREIGHT_DELIVERY_TIME
            JSONArray deliverTimes = (JSONArray)getDataByType(freight, KEY_TYPE_ARRAY,
                    "deliveryTime", path + ":freight[deliveryTime]");
            for (int i = 0; i < deliverTimes.length(); i++){
                JSONObject deliverTime = (JSONObject) deliverTimes.get(i);
                Object row_freight[] = new Object[num_cols_freight];
                param = 0;
                row_freight[param++] = hashPartition(getDataByType(checkout, KEY_TYPE_VARCHAR,
                        "id", path + ":checkout[partition_key]"));
                row_freight[param++] = getDataByType(checkout, KEY_TYPE_VARCHAR,
                        "id", path + ":checkout[id]");
                row_freight[param++] = getDataByType(deliverTime, KEY_TYPE_VARCHAR,
                        "lineId", path + ":deliverTime[lineId]");
                row_freight[param++] = getDataByType(deliverTime, KEY_TYPE_INTEGER,
                        "time", path + ":deliverTime[time]");

                vt_freight.addRow(row_freight);
            }


//            load table CHECKOUT_STOCK_TRANSACTIONS
            JSONArray stockTransactions = (JSONArray)getDataByType(checkout, KEY_TYPE_ARRAY,
                    "stockTransactions", path + ":checkout[stockTransactions]");
            for (int i = 0; i < stockTransactions.length(); i++){
                JSONObject stockTransaction = (JSONObject) stockTransactions.get(i);
                Object row_stock[] = new Object[num_cols_stock];
                param = 0;
                row_stock[param++] = hashPartition(getDataByType(checkout, KEY_TYPE_VARCHAR,
                        "id", path + ":checkout[partition_key]"));
                row_stock[param++] = getDataByType(checkout, KEY_TYPE_VARCHAR,
                        "id", path + ":checkout[id]");
                row_stock[param++] = getDataByType(stockTransaction, KEY_TYPE_VARCHAR,
                        "id", path + ":stockTransaction[id]");
                row_stock[param++] = getDataByType(stockTransaction, KEY_TYPE_VARCHAR,
                        "lineId", path + ":stockTransaction[lineId]");

                vt_stock.addRow(row_stock);
            }

            batchSize++;

            if ((batchSize % configCommitCount) == 0) {
                this.loadVoltTable(catalog_tbl_checkout.getName(), vt_checkout);
                this.loadVoltTable(catalog_tbl_freight.getName(), vt_freight);
                this.loadVoltTable(catalog_tbl_payments.getName(), vt_payments);
                this.loadVoltTable(catalog_tbl_stock.getName(), vt_stock);
                vt_checkout.clearRowData();
                vt_freight.clearRowData();
                vt_payments.clearRowData();
                vt_stock.clearRowData();

                batchSize = 0;
                if (LOG.isDebugEnabled())
                    LOG.debug("Checkout  % " + total);
                else if ((total % 100000) == 0) {
                    LOG.info("Checkout  % " + total);
                }
            }
        }

        if (batchSize > 0) {
            this.loadVoltTable(catalog_tbl_checkout.getName(), vt_checkout);
            this.loadVoltTable(catalog_tbl_freight.getName(), vt_freight);
            this.loadVoltTable(catalog_tbl_payments.getName(), vt_payments);
            this.loadVoltTable(catalog_tbl_stock.getName(), vt_stock);
            vt_checkout.clearRowData();
            vt_freight.clearRowData();
            vt_payments.clearRowData();
            vt_stock.clearRowData();
        }

        if (LOG.isDebugEnabled()) LOG.debug("[Checkouts Loaded] "+total);
        in.close();
    }

    /**
     * This function is used to read the file has the format like :
     * sku;id;warehouse;sub_inventory;stock_type;store_id;lead_time
     * e.g.:
     * 163213;f9705d24-a125-4429-8626-16fc98cfe784;77;1001;1;B2W;1
     * and load it to the VoltTable.
     * @param name the name of the table we want to modified.
     * @param path the path of the file we want to read.
     * @param types the types list of the table, an example is this.STOCK_TRANSACTION_TYPES
     */
    private void loadTableFormatData(Database catalog_db, String name, String path, int[] types)
            throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(
                new FileInputStream(path)));

        Table catalog_tbl_stock = catalog_db.getTables().getIgnoreCase(name);
        assert(catalog_tbl_stock != null);
        VoltTable vt_stock = CatalogUtil.getVoltTable(catalog_tbl_stock);
        int num_cols_stock = catalog_tbl_stock.getColumns().size();

        String line;
        int total = 0;
        int batchSize = 0;

        while (true){
            line = in.readLine();
            if (line == null || line.isEmpty())
                break;
            String[] items = line.split(";");

            Object row_stock[] = new Object[num_cols_stock];
            int param;
            row_stock[0] = hashPartition(getDataByType(items[0], types[0]));
            for (param = 0; param < num_cols_stock - 1; param++){
                row_stock[param + 1] = getDataByType(items[param], types[param]);
            }
            vt_stock.addRow(row_stock);

            total++;
            batchSize++;

            if ((batchSize % configCommitCount) == 0) {
                this.loadVoltTable(catalog_tbl_stock.getName(), vt_stock);
                vt_stock.clearRowData();

                batchSize = 0;
                if (LOG.isDebugEnabled())
                    LOG.debug(name + "  : " + total);
                else if ((total % 100000) == 0) {
                    LOG.info(name + "  : " + total);
                }
            }
        }

        if (batchSize > 0) {
            this.loadVoltTable(catalog_tbl_stock.getName(), vt_stock);
            vt_stock.clearRowData();
        }

        if (LOG.isDebugEnabled()) LOG.debug("[" + name + " Loaded] "+total);

        in.close();
    }

    void loadStockData(Database catalog_db) throws IOException {
        this.loadTableFormatData(catalog_db, B2WConstants.TABLENAME_INVENTORY_STOCK,
                config.stock_inventory_data_file, INVENTORY_STOCK_TYPES);
        this.loadTableFormatData(catalog_db, B2WConstants.TABLENAME_INVENTORY_STOCK_QUANTITY,
                config.stock_quantity_data_file, STOCK_QUANTITY_TYPES);
        this.loadTableFormatData(catalog_db, B2WConstants.TABLENAME_STOCK_TRANSACTION,
                config.stock_transaction_data_file, STOCK_TRANSACTION_TYPES);
    }

    @Override
    public void load() throws IOException {
        boolean b = debug.val;
        debug.set(true);
        if (debug.val) {
            LOG.debug("Starting B2WLoader");
        }
        final CatalogContext catalogContext = this.getCatalogContext();
        try {
            this.loadCartData(catalogContext.database,config.cart_data_file);
            this.loadCheckOutData(catalogContext.database,config.checkout_data_file);
        } catch (JSONException e) {
            LOG.error("JSON load failed");
            e.printStackTrace();
        }
        this.loadStockData(catalogContext.database);
        LOG.info("Load success!!");
        debug.set(b);
    }


 
}
