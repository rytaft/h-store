package edu.mit.benchmark.b2w;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.TimestampType;

import edu.brown.api.BenchmarkComponent;
import edu.brown.api.ControlState;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ThreadUtil;

import static edu.mit.benchmark.b2w.B2WLoader.hashPartition;

public class B2WClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(B2WClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public static enum Transaction {
        ADD_LINE_TO_CART("AddLineToCart"),
        ADD_LINE_TO_CHECKOUT("AddLineToCheckout"),
        CANCEL_STOCK_RESERVATION("CancelStockReservation"),
        CREATE_CHECKOUT("CreateCheckout"),
        CREATE_CHECKOUT_PAYMENT("CreateCheckoutPayment"),
        CREATE_STOCK_TRANSACTION("CreateStockTransaction"),
        DELETE_CART("DeleteCart"),
        DELETE_CHECKOUT("DeleteCheckout"),
        DELETE_LINE_FROM_CART("DeleteLineFromCart"),
        DELETE_LINE_FROM_CHECKOUT("DeleteLineFromCheckout"),
        GET_CART("GetCart"),
        GET_CHECKOUT("GetCheckout"),
        GET_STOCK("GetStock"),
        GET_STOCK_QUANTITY("GetStockQuantity"),
        GET_STOCK_TRANSACTION("GetStockTransaction"),
        PURCHASE_STOCK("PurchaseStock"),
        RESERVE_CART("ReserveCart"),
        RESERVE_STOCK("ReserveStock"),
        UPDATE_STOCK_TRANSACTION("UpdateStockTransaction");
        
        /**
         * Constructor
         */
        private Transaction(String displayName) {
            this.displayName = displayName;
            this.callName = displayName.replace(" ", "");
        }
        
        public final String displayName;
        public final String callName;
    
    } // TRANSCTION ENUM

    public static enum Operation {
        ADD_LINE_TO_CART,
        CANCEL_STOCK_TRANSACTION,
        CHECKOUT,
        DELETE_CART,
        DELETE_CHECKOUT,
        DELETE_LINE_FROM_CART,
        FINISH_STOCK_TRANSACTION,
        GET_CART,
        GET_CHECKOUT,
        GET_STOCK,
        GET_STOCK_QUANTITY,
        GET_STOCK_TRANSACTION,
        PURCHASE;

    } // OPERATION ENUM

    
    
    public static void main(String args[]) {
        BenchmarkComponent.main(B2WClient.class, args, false);
    }


    private B2WConfig config;
    private TransactionSelector txn_selector;
    private ConcurrentHashMap<String,StockIdCacheElement> stock_id_cache; // sku -> stock_ids
    
    // Cache element to contain stock IDs. Cache elements expire after 5 minutes.
    private class StockIdCacheElement {
       public long timestamp; // milliseconds since epoch
       public HashSet<String> stock_ids;
       
       public StockIdCacheElement(long timestamp, HashSet<String> stock_ids) {
           this.timestamp = timestamp;
           this.stock_ids = stock_ids;
       }
    };

    /**
     *  Time of first transaction in milliseconds
     */
    private long startTime;
  

    public B2WClient(String[] args) {
        super(args);     
        this.config = new B2WConfig(m_extraParams);
        this.stock_id_cache = new ConcurrentHashMap<>();
        try {
            this.txn_selector = TransactionSelector.getTransactionSelector(this.config.operations_file);
        } catch (FileNotFoundException e) {
            LOG.error("File not found: " + this.config.operations_file + ". Stack trace: " + e.getStackTrace(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void runLoop() {
        try {
            Client client = this.getClientHandle();

            startTime = System.currentTimeMillis();
            final boolean doSingle = this.getHStoreConf().client.runOnce;
            boolean hadErrors = false;
            boolean bp = false;
	        long offset = 0;
	        JSONObject next_txn = null;
            while (true) {
                // If there is back pressure don't send any requests. Update the
                // last request time so that a large number of requests won't
                // queue up to be sent when there is no longer any back
                // pressure.
                if (bp) {
                    client.backpressureBarrier();
                    bp = false;
                }

                // Check whether we are currently being paused
                // We will block until we're allowed to go again
                if (this.m_controlState == ControlState.PAUSED) {
                    if (debug.val) LOG.debug("Pausing until control lock is released");
                    this.m_pauseLock.acquire();
                    if (debug.val) LOG.debug("Control lock is released! Resuming execution! Tiger style!");
                }
                assert(this.m_controlState != ControlState.PAUSED) : "Unexpected " + this.m_controlState;

                if (next_txn == null) {
                    try {
                    next_txn = txn_selector.nextTransaction();
                    if (next_txn == null) return;
                    offset = next_txn.getLong(B2WConstants.OPERATION_OFFSET) / config.speed_up;
                    } catch (JSONException e) {
                        LOG.error("Failed to parse transaction: " + e.getMessage(), e);
                        continue;
                    }
                }

                // Wait to generate the transaction until sufficient time has passed
                final long now = System.currentTimeMillis();
                final long delta = now - startTime;
                if (delta >= offset) {
                    try {
                        bp = !this.runOnce(next_txn);
                        next_txn = null; // indicates that we are ready for the next txn

                        if (doSingle) {
                            LOG.warn("Stopping client due to a run once setting");
                            break;
                        }
                    } catch (final IOException e) {
                        if (hadErrors) return;
                        hadErrors = true;

                        // HACK: Sleep for a little bit to give time for the site logs to flush
                        LOG.error("Failed to execute transaction: " + e.getMessage(), e);
                        ThreadUtil.sleep(5000);
                    } catch (final JSONException e) {
                        LOG.error("Failed to parse transaction: " + e.getMessage(), e);
                    }
                }
                else {
                    Thread.sleep(25);
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    
    protected boolean runOnce(JSONObject txn) throws IOException, JSONException {
        Operation operation = Operation.valueOf(txn.getString(B2WConstants.OPERATION));
        JSONObject params = txn.getJSONObject(B2WConstants.OPERATION_PARAMS);
        if (trace.val) {
            LOG.trace("Running operation " + txn.getString(B2WConstants.OPERATION) + " with params: " + params.toString());
        }
        switch (operation) {
            case ADD_LINE_TO_CART:
                return runAddLineToCart(params);
            case CANCEL_STOCK_TRANSACTION:
                return runCancelStockTransaction(params);
            case CHECKOUT:
                return runCheckout(params);
            case DELETE_CART:
                return runDeleteCart(params);
            case DELETE_CHECKOUT:
                return runDeleteCheckout(params);
            case DELETE_LINE_FROM_CART:
                return runDeleteLineFromCart(params);
            case FINISH_STOCK_TRANSACTION:
                return runFinishStockTransaction(params);
            case GET_CART:
                return runGetCart(params);
            case GET_CHECKOUT:
                return runGetCheckout(params);
            case GET_STOCK:
                return runGetStock(params);
            case GET_STOCK_QUANTITY:
                return runGetStockQuantity(params);
            case GET_STOCK_TRANSACTION:
                return runGetStockTransaction(params);
            case PURCHASE:
                return runPurchase(params);
            default:
                throw new RuntimeException("Unexpected operation '" + operation + "'");
        }
    }
    
    /**********************************************************/
    /** Convenience methods to convert JSONObjects to values **/
    /**********************************************************/
    private String getString(JSONObject obj, String key) throws JSONException {
        if (obj == null || !obj.has(key)){
            return null;
        }
        return obj.getString(key);
    }
    
    private byte getBoolean(JSONObject obj, String key) throws JSONException {
        if (obj == null || !obj.has(key)){
            return VoltType.NULL_TINYINT;
        }
        return (byte) (obj.getBoolean(key) ? 1 : 0);
    }
    
    private int getInteger(JSONObject obj, String key) throws JSONException {
        if (obj == null || !obj.has(key)){
            return VoltType.NULL_INTEGER;
        }
        return obj.getInt(key);
    }
    
    private long getLong(JSONObject obj, String key) throws JSONException {
        if (obj == null || !obj.has(key)){
            return VoltType.NULL_BIGINT;
        }
        return obj.getLong(key);
    }
    
    private double getDouble(JSONObject obj, String key) throws JSONException {
        if (obj == null || !obj.has(key)){
            return VoltType.NULL_FLOAT;
        }
        return obj.getDouble(key);
    }
    
    private JSONArray getArray(JSONObject obj, String key) throws JSONException {
        if (obj == null || !obj.has(key)){
            return new JSONArray();
        }
        return obj.getJSONArray(key);
    }
    
    private HashSet<String> getStockIds(String sku) throws IOException {
        // cache element expires after 5 minutes
        final int FIVE_MINUTES = 300000; // 1000 ms/s * 60 s/min * 5 min
        if (this.stock_id_cache.contains(sku) && 
                this.stock_id_cache.get(sku).timestamp > (System.currentTimeMillis() - FIVE_MINUTES)) {
            if (debug.val) {
                LOG.debug("Returning stock IDs for sku " + sku + " from the cache: " + this.stock_id_cache.get(sku).stock_ids.toString());
            }
            return this.stock_id_cache.get(sku).stock_ids;
        }
        
        Object getStockParams[] = { hashPartition(sku), sku };
        /**** TRANSACTION ****/
        ClientResponse getStockResponse = runSynchTransaction(Transaction.GET_STOCK, getStockParams);
        if (getStockResponse.getResults().length != 1) {
            if (debug.val) {
                LOG.debug("GetStock response has incorrect number of results (" + getStockResponse.getResults().length + " != 1)");
            }
            return null;
        }
        
        HashSet<String> stock_ids = new HashSet<>();
        for (int i = 0; i < getStockResponse.getResults()[0].getRowCount(); ++i) {
            final VoltTableRow stock = getStockResponse.getResults()[0].fetchRow(i);
            final int STOCK_ID = 1 + 1;
            stock_ids.add(stock.getString(STOCK_ID));
        }
        
        if (debug.val) {
            LOG.debug("Adding stock IDs for sku " + sku + " to the cache: " + stock_ids.toString());
        }
        this.stock_id_cache.put(sku, new StockIdCacheElement(System.currentTimeMillis(), stock_ids));
        
        return stock_ids;
    }
    
    // Reserve stock and create a stock transaction
    private int reserveStock(JSONArray reserves, String product_sku, String transaction_id, int requested_quantity, TimestampType cartTimestamp) 
            throws IOException, JSONException {
        
        int reserve_count = reserves.length();
        
        int total_reserved_quantity = 0;
        String[] reserve_id = new String[reserve_count]; 
        String[] brand = new String[reserve_count];
        TimestampType[] timestamp = new TimestampType[reserve_count];
        TimestampType[] expiration_date = new TimestampType[reserve_count];
        byte[] is_kit = new byte[reserve_count];
        String[] reserve_lines = new String[reserve_count];
        int[] reserved_quantity = new int[reserve_count];
        String[] sku = new String[reserve_count];
        String[] store_id = new String[reserve_count];
        int[] subinventory = new int[reserve_count];
        int[] warehouse = new int[reserve_count];
        
        HashSet<String> stock_ids = getStockIds(product_sku);
        // This code only tries to reserve stock_ids that we know succeeded from the input parameters.
        // It may be more realistic to try some of the other stock_ids, but the priority depends on some complicated
        // logic about where the warehouse is located relative to the customer, stock type, and total available....

        for (int j = 0; j < reserve_count; ++j) {
            JSONObject reserve = reserves.getJSONObject(j);
            String stock_id = getString(reserve, B2WConstants.PARAMS_STOCK_ID); 
            if (!stock_ids.contains(stock_id)) {
                LOG.info("Attempting to reserve stock_id " + stock_id + " which is not present in the cache for sku " + sku);
            }
            if (requested_quantity != getInteger(reserve, B2WConstants.PARAMS_REQUESTED_QUANTITY)) {
                LOG.info("Requested quantity in database <" + requested_quantity + "> doesn't match log <" + getInteger(reserve, B2WConstants.PARAMS_REQUESTED_QUANTITY) +">");
            }
         
            // Attempt to reserve the stock
            Object reserveStockParams[] = { hashPartition(stock_id), stock_id, requested_quantity };
            /**** TRANSACTION ****/
            ClientResponse reserveStockResponse = runSynchTransaction(Transaction.RESERVE_STOCK, reserveStockParams);
            if (reserveStockResponse.getResults().length != 1 || 
                    reserveStockResponse.getResults()[0].getRowCount() != 1) {
                if (debug.val) {
                    LOG.debug("ReserveStock response has incorrect number of results (" + reserveStockResponse.getResults().length 
                            + ") or incorrect number of rows");
                }
                return total_reserved_quantity;
            }
            reserved_quantity[j] = (int) reserveStockResponse.getResults()[0].fetchRow(0).getLong(0);
            if (trace.val) {
                LOG.trace("Successfully reserved " + reserved_quantity[j] + " of stock ID " + stock_id + " (requested " + requested_quantity + ")");
            } 
            if (reserved_quantity[j] != getInteger(reserve, B2WConstants.PARAMS_RESERVED_QUANTITY)) {
                LOG.info("Reserved quantity in database <" + reserved_quantity[j] + "> doesn't match log <" + getInteger(reserve, B2WConstants.PARAMS_RESERVED_QUANTITY) +">");
            }

            // If successfully reserved, create a stock transaction reserve
            if (reserved_quantity[j] > 0) {
                reserve_id[j] = getString(reserve, B2WConstants.PARAMS_RESERVE_ID); 
                brand[j] = getString(reserve, B2WConstants.PARAMS_BRAND);
                timestamp[j] = new TimestampType(getLong(reserve, B2WConstants.PARAMS_CREATION_DATE));
                if (timestamp[j].getMSTime() > cartTimestamp.getMSTime()) cartTimestamp = timestamp[j];
                expiration_date[j] = new TimestampType(new Date(timestamp[j].getMSTime() + 30 * 60 * 1000)); // add 30 mins
                is_kit[j] = getBoolean(reserve, B2WConstants.PARAMS_IS_KIT);
                reserve_lines[j] = getString(reserve, B2WConstants.PARAMS_RESERVE_LINES);
                sku[j] = getString(reserve, B2WConstants.PARAMS_SKU);
                store_id[j] = getString(reserve, B2WConstants.PARAMS_STORE_ID);
                subinventory[j] = getInteger(reserve, B2WConstants.PARAMS_SUBINVENTORY);
                warehouse[j] = getInteger(reserve, B2WConstants.PARAMS_WAREHOUSE);

                total_reserved_quantity += reserved_quantity[j];
            }
        }
        
        // Create the stock transaction
        Object createStockTxnParams[] = { hashPartition(transaction_id), transaction_id, reserve_id, brand, timestamp, 
                expiration_date, is_kit, requested_quantity, reserve_lines, reserved_quantity, sku, 
                store_id, subinventory, warehouse };
        /**** TRANSACTION ****/
        runAsynchTransaction(Transaction.CREATE_STOCK_TRANSACTION, createStockTxnParams);               
        
        return total_reserved_quantity;
    }
    
    // Cancel stock reservations and cancel the stock transaction
    private boolean cancelStockTransaction(String stockTransactionId, TimestampType timestamp) throws IOException, JSONException {
        // cancel stock transaction
        String current_status = B2WConstants.STATUS_CANCELLED;
        Object updateStockTxnParams[] = { hashPartition(stockTransactionId), stockTransactionId, timestamp, current_status };
        /**** TRANSACTION ****/
        ClientResponse cancelStockTransactionResponse = runSynchTransaction(Transaction.UPDATE_STOCK_TRANSACTION, updateStockTxnParams); 
        if (cancelStockTransactionResponse.getResults().length != 1 || 
                cancelStockTransactionResponse.getResults()[0].getRowCount() != 1) {
            if (debug.val) {
                LOG.debug("UpdateStockTransaction response has incorrect number of results (" + cancelStockTransactionResponse.getResults().length 
                        + ") or incorrect number of rows");
            }
            return false;
        }
        boolean status_changed = cancelStockTransactionResponse.getResults()[0].fetchRow(0).getBoolean(0);
        if (trace.val) {
            LOG.trace("Txn ID " + stockTransactionId + (status_changed ? " successfully cancelled" : " was already cancelled"));
        }       

        // cancel stock reservations
        if (status_changed) { // if no change, someone else already cancelled the transaction and canceled the reservations
            Object getStockTxnParams[] = { hashPartition(stockTransactionId), stockTransactionId };
            /**** TRANSACTION ****/
            ClientResponse getStockTxnResponse = runSynchTransaction(Transaction.GET_STOCK_TRANSACTION, getStockTxnParams);
            if (getStockTxnResponse.getResults().length != 1)  {
                if (debug.val) {
                    LOG.debug("GetStockTransaction response has incorrect number of results (" + getStockTxnResponse.getResults().length + " != 1)");
                }
                return false;
            }
            if (trace.val) {
                LOG.trace("StockTransaction for txn ID " + stockTransactionId + ": " + getStockTxnResponse.getResults()[0].toString());
            }
            
            for (int j = 0; j < getStockTxnResponse.getResults()[0].getRowCount(); ++j) {
                final VoltTableRow stockTransaction = getStockTxnResponse.getResults()[0].fetchRow(j);
                final int RESERVE_LINES = 8 + 1;

                String reserve_lines = stockTransaction.getString(RESERVE_LINES);
                JSONObject reserve_lines_obj = new JSONObject(reserve_lines);
                String stock_id = reserve_lines_obj.getString(B2WConstants.PARAMS_STOCK_ID);
                int reserved_quantity = reserve_lines_obj.getInt(B2WConstants.PARAMS_RESERVED_QUANTITY);
                Object cancelReserveStockParams[] = { hashPartition(stock_id), stock_id, reserved_quantity };
                /**** TRANSACTION ****/
                boolean success = runAsynchTransaction(Transaction.CANCEL_STOCK_RESERVATION, cancelReserveStockParams);
                if (!success) return false;
            }
        }
        
        return true;
    }
    
    // Example JSON
    //
    // {
    //   "operation": "ADD_LINE_TO_CART",
    //   "offset": <milliseconds>,
    //   "params": {
    //      "cartId": <cart_id>,
    //      "lineId": <line_id>,
    //      "timestamp": <timestamp>, // microseconds since epoch
    //      "productSku": <product_sku>,
    //      "productId": <product_id>,
    //      "storeId": <store_id>,
    //      "quantity": <store_id>,
    //      "salesChannel": <sales_channel>,
    //      "opn": <opn>,
    //      "epar": <epar>,
    //      "autoMerge": <auto_merge>,       
    //      "unitSalesPrice": <unit_sales_price>,
    //      "salesPrice": <sales_price>,
    //      "maxQuantity": <max_quantity>,
    //      "maximumQuantityReason": <maximum_quantity_reason>,
    //      "type": <type>,
    //      "stockTransactionId": <stock_transaction_id>,
    //      "requestedQuantity": <requested_quantity>,
    //      "lineStatus": <line_status>,
    //      "stockType": <stock_type>,
    //      "image": <stock_type>,
    //      "name": <name>,
    //      "isKit": <name>,
    //      "price": <price>,
    //      "originalPrice": <original_price>,
    //      "isLarge": <is_large>,
    //      "department": <department>,
    //      "line": <line>,
    //      "subClass": <sub_class>,
    //      "weight": <weight>,
    //      "productClass": <product_class>
    //      "checkoutId": <checkout_id> // optional
    //      "freightContract": <freight_contract>, // optional 
    //      "freightPrice": <freight_price>, // optional 
    //      "freightStatus": <freight_status> // optional 
    //      "deliveryTime": <delivery_time>, // optional
    //      "reserves": [{ // optional
    //          "stockId": <stock_id>, 
    //          "reserveId": <reserve_id>, 
    //          "brand": <brand>, 
    //          "stockTxnCreationTimestamp": <creation_date>, // microseconds since epoch
    //          "isKit": <is_kit>, 
    //          "requestedQuantity": <requested_quantity>, 
    //          "reserveLines": <reserve_lines>, 
    //          "reservedQuantity": <reserved_quantity>, 
    //          "sku": <sku>, 
    //          "storeId": <store_id>, 
    //          "subinventory": <subinventory>, 
    //          "warehouse": <warehouse>
    //      },{
    //          ...
    //      }]
    //   }
    // }
    private boolean runAddLineToCart(JSONObject params) throws IOException, JSONException {
        String cart_id = getString(params, B2WConstants.PARAMS_CART_ID);
        TimestampType timestamp = new TimestampType(getLong(params, B2WConstants.PARAMS_TIMESTAMP));
        String line_id = getString(params, B2WConstants.PARAMS_LINE_ID);
        String product_sku = getString(params, B2WConstants.PARAMS_PRODUCT_SKU); 
        long product_id = getLong(params, B2WConstants.PARAMS_PRODUCT_ID);
        String store_id = getString(params, B2WConstants.PARAMS_STORE_ID);
        int quantity = getInteger(params, B2WConstants.PARAMS_QUANTITY);
        String salesChannel = getString(params, B2WConstants.PARAMS_SALES_CHANNEL);
        String opn = getString(params, B2WConstants.PARAMS_OPN);
        String epar = getString(params, B2WConstants.PARAMS_EPAR);
        byte autoMerge = getBoolean(params, B2WConstants.PARAMS_AUTO_MERGE);        
        double unitSalesPrice = getDouble(params, B2WConstants.PARAMS_UNIT_SALES_PRICE);
        double salesPrice = getDouble(params, B2WConstants.PARAMS_SALES_PRICE);
        int maxQuantity = getInteger(params, B2WConstants.PARAMS_MAX_QUANTITY);
        String maximumQuantityReason = getString(params, B2WConstants.PARAMS_MAXIMUM_QUANTITY_REASON);
        String type = getString(params, B2WConstants.PARAMS_TYPE);
        String transaction_id = getString(params, B2WConstants.PARAMS_STOCK_TRANSACTION_ID);
        int requested_quantity = getInteger(params, B2WConstants.PARAMS_REQUESTED_QUANTITY);
        String line_status = getString(params, B2WConstants.PARAMS_LINE_STATUS);
        String stockType = getString(params, B2WConstants.PARAMS_STOCK_TYPE);
        String image = getString(params, B2WConstants.PARAMS_IMAGE);
        String name = getString(params, B2WConstants.PARAMS_NAME);
        byte isKit = getBoolean(params, B2WConstants.PARAMS_IS_KIT);
        double price = getDouble(params, B2WConstants.PARAMS_PRICE);
        double originalPrice = getDouble(params, B2WConstants.PARAMS_ORIGINAL_PRICE);
        byte isLarge = getBoolean(params, B2WConstants.PARAMS_IS_LARGE);
        long department = getLong(params, B2WConstants.PARAMS_DEPARTMENT);
        long line = getLong(params, B2WConstants.PARAMS_LINE);
        long subClass = getLong(params, B2WConstants.PARAMS_SUB_CLASS);
        double weight = getDouble(params, B2WConstants.PARAMS_WEIGHT);
        long product_class = getLong(params, B2WConstants.PARAMS_PRODUCT_CLASS);
        
        // if necessary, reserve stock
        JSONArray reserves = getArray(params, B2WConstants.PARAMS_RESERVES);
        if (reserves != null && transaction_id != null && !transaction_id.isEmpty()) {
            TimestampType cartTimestamp = new TimestampType(0);
            int total_reserved_quantity = reserveStock(reserves, product_sku, transaction_id, requested_quantity, cartTimestamp);
            
            String actual_line_status = (requested_quantity == total_reserved_quantity ? B2WConstants.STATUS_COMPLETE : B2WConstants.STATUS_INCOMPLETE);  
            if (actual_line_status != line_status) {
                LOG.info("Actual line status in database <" + actual_line_status + "> doesn't match log <" + line_status +">");
            } 
        } 
        // otherwise check that the stock is available
        else {
            HashSet<String> stockIds = getStockIds(product_sku);
            int total_available = 0;
            for(String stockId : stockIds) {
                Object getStockQtyParams[] = { hashPartition(stockId), stockId };
                /**** TRANSACTION ****/
                ClientResponse stockQtyResponse = runSynchTransaction(Transaction.GET_STOCK_QUANTITY, getStockQtyParams);
                if (stockQtyResponse.getResults().length != 1 || stockQtyResponse.getResults()[0].getRowCount() != 1) {
                    if (debug.val) {
                        LOG.debug("GetStockQuantity response has incorrect number of results (" + stockQtyResponse.getResults().length 
                                + ") or incorrect number of rows");
                    }
                    return false; 
                }
                if (trace.val) {
                    LOG.trace("StockQty for stock ID " + stockId + ": " + stockQtyResponse.getResults()[0].toString());
                }
                
                final VoltTableRow stockQty = stockQtyResponse.getResults()[0].fetchRow(0);
                final int AVAILABLE = 1 + 1;
                total_available += stockQty.getLong(AVAILABLE);
                if (total_available >= requested_quantity) break;
            }
            
            if (total_available < requested_quantity) {
                LOG.info("Total available (" + total_available + ") less than requested (" + requested_quantity + ")");
                if (total_available > 0) {
                    requested_quantity = total_available;
                } else { // no point in adding to cart
                    return true;
                }
            }
        }
        
        // if necessary, add line to checkout
        String checkout_id = getString(params, B2WConstants.PARAMS_CHECKOUT_ID);
        if (checkout_id != null && !checkout_id.isEmpty()) {
            String freightContract = getString(params, B2WConstants.PARAMS_FREIGHT_CONTRACT);
            double freightPrice = getDouble(params, B2WConstants.PARAMS_FREIGHT_PRICE);
            String freightStatus = getString(params, B2WConstants.PARAMS_FREIGHT_STATUS);
            int delivery_time = getInteger(params, B2WConstants.PARAMS_DELIVERY_TIME);

            Object addCheckoutLineParams[] = { hashPartition(checkout_id), checkout_id, line_id, salesPrice, transaction_id, delivery_time, 
                    freightContract, freightPrice, freightStatus };
            /**** TRANSACTION ****/
            boolean success = runAsynchTransaction(Transaction.ADD_LINE_TO_CHECKOUT, addCheckoutLineParams);
            if (!success) return false;
        }
        
        // add line to cart
        Object addLineParams[] = { hashPartition(cart_id), cart_id, timestamp, line_id, 
                product_sku, product_id, store_id, quantity, salesChannel, opn, epar, autoMerge,
                unitSalesPrice, salesPrice, maxQuantity, maximumQuantityReason, type, transaction_id,
                requested_quantity, line_status, stockType, image, name, isKit, price, originalPrice,
                isLarge, department, line, subClass, weight, product_class };
        /**** TRANSACTION ****/
        return runAsynchTransaction(Transaction.ADD_LINE_TO_CART, addLineParams);   
    }    
    
    // Example JSON
    //
    // {
    //   "operation": "CANCEL_STOCK_TRANSACTION",
    //   "offset": <milliseconds>,
    //   "params": {
    //      "transactionId": <transaction_id>,
    //      "timestamp": <timestamp>, // microseconds since epoch
    //   }
    // }
    private boolean runCancelStockTransaction(JSONObject params) throws IOException, JSONException {
        TimestampType timestamp = new TimestampType(getLong(params, B2WConstants.PARAMS_TIMESTAMP));
        String stockTransactionId = getString(params, B2WConstants.PARAMS_TRANSACTION_ID);
        return cancelStockTransaction(stockTransactionId, timestamp);
    }

    
    // Example JSON
    //
    // {
    //   "operation": "DELETE_LINE_FROM_CART",
    //   "offset": <milliseconds>,
    //   "params": {
    //      "cartId": <cart_id>,
    //      "lineId": <line_id>,
    //      "timestamp": <timestamp>, // microseconds since epoch
    //      "checkoutId": <checkout_id>, // optional
    //      "freightContract": <freight_contract>, // optional 
    //      "freightPrice": <freight_price>, // optional 
    //      "freightStatus": <freight_status> // optional 
    //   }
    // }
    private boolean runDeleteLineFromCart(JSONObject params) throws IOException, JSONException {
        
        String cart_id = getString(params, B2WConstants.PARAMS_CART_ID);
        String line_id = getString(params, B2WConstants.PARAMS_LINE_ID);
        TimestampType timestamp = new TimestampType(getLong(params, B2WConstants.PARAMS_TIMESTAMP));
        Object cartParams[] = { hashPartition(cart_id), cart_id };
        /**** TRANSACTION ****/
        ClientResponse cartResponse = runSynchTransaction(Transaction.GET_CART, cartParams);
        if (cartResponse.getResults().length != B2WConstants.CART_TABLE_COUNT) {
            if (debug.val) {
                LOG.debug("GetCart response has incorrect number of results (" + cartResponse.getResults().length + " != " 
                        + B2WConstants.CART_TABLE_COUNT + ")");
            }
            return false;        
        }
        final int CART_LINES_RESULTS = 2 + 1;
        
        VoltTableRow cartLine = null;
        final int LINE_ID = 1 + 1, SALES_PRICE = 6 + 1, STOCK_TRANSACTION_ID = 11 + 1;
        for (int i = 0; i < cartResponse.getResults()[CART_LINES_RESULTS].getRowCount(); ++i) {
            cartLine = cartResponse.getResults()[CART_LINES_RESULTS].fetchRow(i);
            if (cartLine.getString(LINE_ID).equals(line_id)) {
                break;
            }
        }        
        if (cartLine == null) return false;
        
        // if necessary, cancel the stock transaction
        String stockTransactionId = cartLine.getString(STOCK_TRANSACTION_ID);
        if (!cartLine.wasNull() && stockTransactionId != null) {
            boolean success = cancelStockTransaction(stockTransactionId, timestamp);
            if (!success) return false;
        }
        
        // If necessary, delete lines from checkout
        String checkout_id = getString(params, B2WConstants.PARAMS_CHECKOUT_ID);
        if (checkout_id != null && !checkout_id.isEmpty()) {
            double salesPrice = cartLine.getDouble(SALES_PRICE);
            String freightContract = getString(params, B2WConstants.PARAMS_FREIGHT_CONTRACT);
            double freightPrice = getDouble(params, B2WConstants.PARAMS_FREIGHT_PRICE);
            String freightStatus = getString(params, B2WConstants.PARAMS_FREIGHT_STATUS);
            
            Object deleteCheckoutLineParams[] = { hashPartition(checkout_id), checkout_id, line_id, salesPrice, freightContract, freightPrice, freightStatus };
            /**** TRANSACTION ****/
            boolean success = runAsynchTransaction(Transaction.DELETE_LINE_FROM_CHECKOUT, deleteCheckoutLineParams);
            if (!success) return false;
        }
        
        // Finally, delete lines from cart
        Object deleteLineParams[] = { hashPartition(cart_id), cart_id, timestamp, line_id };
        /**** TRANSACTION ****/
        return runAsynchTransaction(Transaction.DELETE_LINE_FROM_CART, deleteLineParams);   
    }
    

    
    // Example JSON
    //
    // {
    //   "operation": "CHECKOUT",
    //   "offset": <milliseconds>,
    //   "params": {
    //      "cartId": <cart_id>,
    //      "checkoutId": <checkout_id>,
    //      "customerId": <customer_id>,
    //      "token": <token>, 
    //      "guest": <guest>, 
    //      "isGuest": <is_guest>,
    //      "deliveryAddressId": <delivery_address_id>, 
    //      "billingAddressId": <billing_address_id>, 
    //      "amountDue": <amount_due>, 
    //      "total": <total>, 
    //      "freightContract": <freight_contract>, 
    //      "freightPrice": <freight_price>, 
    //      "freightStatus": <freight_status>, 
    //      "lines": [{
    //        "lineId": <line_id>,
    //        "transactionId": <transaction_id>, 
    //        "productSku": <product_sku>,
    //        "stockType": <stock_type>,
    //        "deliveryTime": <delivery_time>,
    //        "reserves": [{
    //          "stockId": <stock_id>, 
    //          "reserveId": <reserve_id>, 
    //          "brand": <brand>, 
    //          "stockTxnCreationTimestamp": <creation_date>, // microseconds since epoch
    //          "isKit": <is_kit>, 
    //          "requestedQuantity": <requested_quantity>, 
    //          "reserveLines": <reserve_lines>, 
    //          "reservedQuantity": <reserved_quantity>, 
    //          "sku": <sku>, 
    //          "storeId": <store_id>, 
    //          "subinventory": <subinventory>, 
    //          "warehouse": <warehouse>
    //        },{
    //          ...
    //        }]
    //      },{
    //        ... 
    //      }]
    //   }
    // }
    private boolean runCheckout(JSONObject params) throws IOException, JSONException {
        // Get the cart and cart lines
        String cart_id = getString(params, B2WConstants.PARAMS_CART_ID);
        Object cartParams[] = { hashPartition(cart_id), cart_id };
        /**** TRANSACTION ****/
        ClientResponse cartResponse = runSynchTransaction(Transaction.GET_CART, cartParams);
        if (cartResponse.getResults().length != B2WConstants.CART_TABLE_COUNT) {
            if (debug.val) {
                LOG.debug("GetCart response has incorrect number of results (" + cartResponse.getResults().length + " != " 
                        + B2WConstants.CART_TABLE_COUNT + ")");
            }
            return false;        
        }
        final int CART_LINES_RESULTS = 2;
        if (trace.val) {
            LOG.trace("CartLines of cart " + cart_id + ": " + cartResponse.getResults()[CART_LINES_RESULTS].toString());
        }
        
        JSONArray lines = getArray(params, B2WConstants.PARAMS_LINES);
        HashMap<String,JSONObject> lines_map = new HashMap<>();
        for(int i = 0; i < lines.length(); ++i) {
            JSONObject line = lines.getJSONObject(i);
            lines_map.put(line.getString(B2WConstants.PARAMS_LINE_ID), line);
        }
        TimestampType cartTimestamp = new TimestampType(0);
        
        int lines_count = cartResponse.getResults()[CART_LINES_RESULTS].getRowCount();
        String[] line_ids = new String[lines_count];
        int[] requested_quantities = new int[lines_count];
        int[] reserved_quantities = new int[lines_count];
        String[] statuses = new String[lines_count];
        String[] stock_types = new String[lines_count];
        String[] transaction_ids = new String[lines_count];
        int[] delivery_times = new int[lines_count];

        // Iterate through the cart lines, and attempt to reserve each item.  If the item was successfully reserved,
        // Create a new stock transaction.
        for (int i = 0; i < lines_count; ++i) {
            final VoltTableRow cartLine = cartResponse.getResults()[CART_LINES_RESULTS].fetchRow(i);
            final int LINE_ID = 1 + 1, QUANTITY = 7 + 1;
            String line_id = cartLine.getString(LINE_ID);
            if (!lines_map.containsKey(line_id)) {
                LOG.info("No log info for line_id: " + line_id);
                continue;
            }
            JSONObject line = lines_map.get(line_id);
            int requested_quantity = (int) cartLine.getLong(QUANTITY);
            int total_reserved_quantity = 0;
            String transaction_id = line.getString(B2WConstants.PARAMS_TRANSACTION_ID); 
            if (transaction_id == null || transaction_id.isEmpty()) {
                if (debug.val) LOG.debug("No transaction_id for line_id: " + line_id);
            } else {
                // reserve the stock
                String sku = line.getString(B2WConstants.PARAMS_PRODUCT_SKU);
                JSONArray reserves = line.getJSONArray(B2WConstants.PARAMS_RESERVES);
                total_reserved_quantity = reserveStock(reserves, sku, transaction_id, requested_quantity, cartTimestamp);                
            }           
            
            // store the line id and transaction info to add to the cart and checkout
            line_ids[i] = line_id;
            requested_quantities[i] = requested_quantity;
            reserved_quantities[i] = total_reserved_quantity;
            statuses[i] = (requested_quantity == total_reserved_quantity ? B2WConstants.STATUS_COMPLETE : B2WConstants.STATUS_INCOMPLETE);
            stock_types[i] = line.getString(B2WConstants.PARAMS_STOCK_TYPE);
            transaction_ids[i] = transaction_id;
            delivery_times[i] = line.getInt(B2WConstants.PARAMS_DELIVERY_TIME);
        }
        
        
        // Update the cart with the new transactions, customer, etc
        String customer_id = getString(params, B2WConstants.PARAMS_CUSTOMER_ID);
        String token = getString(params, B2WConstants.PARAMS_TOKEN); 
        byte guest = getBoolean(params, B2WConstants.PARAMS_GUEST);
        byte isGuest = getBoolean(params, B2WConstants.PARAMS_IS_GUEST);
        Object reserveCartParams[] = { hashPartition(cart_id), cart_id, cartTimestamp, customer_id, token, guest, isGuest, 
                line_ids, requested_quantities, reserved_quantities, statuses, stock_types, transaction_ids };
        /**** TRANSACTION ****/
        boolean success = runAsynchTransaction(Transaction.RESERVE_CART, reserveCartParams);
        if (!success) return false;

        // Finally, create the checkout object
        String checkout_id = getString(params, B2WConstants.PARAMS_CHECKOUT_ID);
        String deliveryAddressId = getString(params, B2WConstants.PARAMS_DELIVERY_ADDRESS_ID); 
        String billingAddressId = getString(params, B2WConstants.PARAMS_BILLING_ADDRESS_ID);
        double amountDue = getDouble(params, B2WConstants.PARAMS_AMOUNT_DUE); 
        double total = getDouble(params, B2WConstants.PARAMS_TOTAL); 
        String freightContract = getString(params, B2WConstants.PARAMS_FREIGHT_CONTRACT); 
        double freightPrice = getDouble(params, B2WConstants.PARAMS_FREIGHT_PRICE); 
        String freightStatus = getString(params, B2WConstants.PARAMS_FREIGHT_STATUS);
        Object checkoutParams[] = { hashPartition(checkout_id), checkout_id, cart_id, deliveryAddressId, billingAddressId, amountDue, total, 
                freightContract, freightPrice, freightStatus, line_ids, transaction_ids, delivery_times };
        if (trace.val) {
            LOG.trace("Creating checkout with params: " + checkout_id + ", " + cart_id + ", " + deliveryAddressId + ", " + billingAddressId + ", " +
             amountDue + ", " + total + ", " + freightContract + ", " + freightPrice + ", " + freightStatus + ", " +
             Arrays.asList(line_ids).toString() + ", " + Arrays.asList(transaction_ids).toString() + ", " + Arrays.asList(delivery_times).toString());
        }
        
        /**** TRANSACTION ****/
        return runAsynchTransaction(Transaction.CREATE_CHECKOUT, checkoutParams);   
    }
    
    // Example JSON
    //
    // {
    //   "operation": "PURCHASE",
    //   "offset": <milliseconds>,
    //   "params": {
    //      "cartId": <cart_id>,
    //      "checkoutId": <checkout_id>,
    //      "paymentOptionId": <payment_option_id>, 
    //      "paymentOptionType": <payment_option_type>, 
    //      "dueDays": <due_days>, 
    //      "amount": <amount>, 
    //      "installmentQuantity": <installment_quantity>,
    //      "interestAmount": <interest_amount>,
    //      "interestRate": <interest_rate>, 
    //      "annualCET": <annual_cet>, 
    //      "number": <number>, 
    //      "criptoNumber": <cripto_number>, 
    //      "holdersName": <holders_name>, 
    //      "securityCode": <security_code>, 
    //      "expirationDate": <expiration_date>,
    //      "timestamp": <timestamp> // microseconds since epoch
    //   }
    // }
    private boolean runPurchase(JSONObject params) throws IOException, JSONException {
        // Add payment info to the checkout object
        String checkout_id = getString(params, B2WConstants.PARAMS_CHECKOUT_ID);
        String cart_id = getString(params, B2WConstants.PARAMS_CART_ID);
        String paymentOptionId = getString(params, B2WConstants.PARAMS_PAYMENT_OPTION_ID);
        String paymentOptionType = getString(params, B2WConstants.PARAMS_PAYMENT_OPTION_TYPE);
        int dueDays = getInteger(params, B2WConstants.PARAMS_DUE_DAYS);
        double amount = getDouble(params, B2WConstants.PARAMS_AMOUNT);
        int installmentQuantity = getInteger(params, B2WConstants.PARAMS_INSTALLMENT_QUANTITY);
        double interestAmount = getDouble(params, B2WConstants.PARAMS_INTEREST_AMOUNT);
        int interestRate = getInteger(params, B2WConstants.PARAMS_INTEREST_RATE);
        int annualCET = getInteger(params, B2WConstants.PARAMS_ANNUAL_CET);
        String number = getString(params, B2WConstants.PARAMS_NUMBER);
        String criptoNumber = getString(params, B2WConstants.PARAMS_CRIPTO_NUMBER);
        String holdersName = getString(params, B2WConstants.PARAMS_HOLDERS_NAME);
        String securityCode = getString(params, B2WConstants.PARAMS_SECURITY_CODE);
        String expirationDate = getString(params, B2WConstants.PARAMS_EXPIRATION_DATE);

        Object checkoutPaymentParams[] = { hashPartition(checkout_id), checkout_id, cart_id, paymentOptionId, paymentOptionType, dueDays, amount, installmentQuantity,
                interestAmount, interestRate, annualCET, number, criptoNumber, holdersName, securityCode, expirationDate };
        /**** TRANSACTION ****/
        boolean success = runAsynchTransaction(Transaction.CREATE_CHECKOUT_PAYMENT, checkoutPaymentParams);
        if (!success) return false;
        
        // Get all the stock transactions for the purchase from the checkout object
        Object checkoutParams[] = { hashPartition(checkout_id), checkout_id };
        /**** TRANSACTION ****/
        ClientResponse checkoutResponse = runSynchTransaction(Transaction.GET_CHECKOUT, checkoutParams);
        if (checkoutResponse.getResults().length != B2WConstants.CHECKOUT_TABLE_COUNT) {
            if (debug.val) {
                LOG.debug("GetCheckout response has incorrect number of results (" + checkoutResponse.getResults().length + " != " 
                        + B2WConstants.CHECKOUT_TABLE_COUNT + ")");
            }
            return false;        
        }
        final int CHECKOUT_STOCK_TRANSACTIONS_RESULTS = 3 + 1;
        if (trace.val) {
            LOG.trace("StockTransactions of checkout " + checkout_id + ": " + 
                    checkoutResponse.getResults()[CHECKOUT_STOCK_TRANSACTIONS_RESULTS].toString());
        }
        
        TimestampType timestamp = new TimestampType(getLong(params, B2WConstants.PARAMS_TIMESTAMP));
        for (int i = 0; i < checkoutResponse.getResults()[CHECKOUT_STOCK_TRANSACTIONS_RESULTS].getRowCount(); ++i) {
            final VoltTableRow checkoutStockTransaction = checkoutResponse.getResults()[CHECKOUT_STOCK_TRANSACTIONS_RESULTS].fetchRow(i);
            final int TRANSACTION_ID = 1 + 1;
            String transaction_id = checkoutStockTransaction.getString(TRANSACTION_ID);
            
            Object getStockTxnParams[] = { hashPartition(transaction_id), transaction_id };
            /**** TRANSACTION ****/
            ClientResponse getStockTxnResponse = runSynchTransaction(Transaction.GET_STOCK_TRANSACTION, getStockTxnParams);
            if (getStockTxnResponse.getResults().length != 1) {
                if (debug.val) {
                    LOG.debug("GetStockTransaction response has incorrect number of results (" + getStockTxnResponse.getResults().length + " != 1)");
                }
                return false;
            }
            if (trace.val) {
                LOG.trace("Stock transaction " + transaction_id + ": " + getStockTxnResponse.getResults()[0].toString());
            }
            
            boolean purchased = false;
            for (int j = 0; j < getStockTxnResponse.getResults()[0].getRowCount(); ++j) {
                final VoltTableRow stockTransaction = getStockTxnResponse.getResults()[0].fetchRow(j);
                final int CURRENT_STATUS = 4 + 1, RESERVE_LINES = 8 + 1;
                
                String current_status = stockTransaction.getString(CURRENT_STATUS);                
                if (current_status.equals(B2WConstants.STATUS_CANCELLED)) {
                    // TODO: try to reserve stock again?
                    LOG.info("Attempt to purchase stock transaction that has been cancelled: " + transaction_id);
                } else {
                    String reserve_lines = stockTransaction.getString(RESERVE_LINES);
                    JSONObject reserve_lines_obj = new JSONObject(reserve_lines);
                    String stock_id = reserve_lines_obj.getString(B2WConstants.PARAMS_STOCK_ID);
                    int reserved_quantity = reserve_lines_obj.getInt(B2WConstants.PARAMS_RESERVED_QUANTITY);
                    Object purchaseStockParams[] = { hashPartition(stock_id), stock_id, reserved_quantity };
                    /**** TRANSACTION ****/
                    success = runAsynchTransaction(Transaction.PURCHASE_STOCK, purchaseStockParams);
                    purchased = true;
                } 
                
                if (!success) return false;
            }
            
            if (purchased) {
                String current_status = B2WConstants.STATUS_PURCHASED;
                Object updateStockTxnParams[] = { hashPartition(transaction_id), transaction_id, timestamp, current_status };
                /**** TRANSACTION ****/
                success = runAsynchTransaction(Transaction.UPDATE_STOCK_TRANSACTION, updateStockTxnParams);               
                if (!success) return false;
            }
        }
        
        return success;
    }
    
    // Example JSON
    //
    // {
    //   "operation": "FINISH_STOCK_TRANSACTION",
    //   "offset": <milliseconds>,
    //   "params": {
    //      "transactionId": <transaction_id>,
    //      "timestamp": <timestamp> // microseconds since epoch
    //   }
    // }
    private boolean runFinishStockTransaction(JSONObject params) throws IOException, JSONException {
        TimestampType timestamp = new TimestampType(getLong(params, B2WConstants.PARAMS_TIMESTAMP));
        String transaction_id = getString(params, B2WConstants.PARAMS_TRANSACTION_ID);
        String current_status = B2WConstants.STATUS_FINISHED;
        Object updateStockTxnParams[] = { hashPartition(transaction_id), transaction_id, timestamp, current_status };
        /**** TRANSACTION ****/
        return runAsynchTransaction(Transaction.UPDATE_STOCK_TRANSACTION, updateStockTxnParams);
    }
    
    // Example JSON
    //
    // {
    //   "operation": "GET_CART",
    //   "offset": <milliseconds>,
    //   "params": {
    //      "cartId": <cart_id>
    //   }
    // }
    private boolean runGetCart(JSONObject params) throws IOException, JSONException {
        String cart_id = getString(params, B2WConstants.PARAMS_CART_ID);
        Object cartParams[] = { hashPartition(cart_id), cart_id };
        return runAsynchTransaction(Transaction.GET_CART, cartParams);      
    }

    // Example JSON
    //
    // {
    //   "operation": "GET_CHECKOUT",
    //   "offset": <milliseconds>,
    //   "params": {
    //      "checkoutId": <checkout_id>
    //   }
    // }
    private boolean runGetCheckout(JSONObject params) throws IOException, JSONException {
        String checkout_id = getString(params, B2WConstants.PARAMS_CHECKOUT_ID);
        Object checkoutParams[] = { hashPartition(checkout_id), checkout_id };
        return runAsynchTransaction(Transaction.GET_CHECKOUT, checkoutParams);
    }

    // Example JSON
    //
    // {
    //   "operation": "DELETE_CART",
    //   "offset": <milliseconds>,
    //   "params": {
    //      "cartId": <cart_id>
    //   }
    // }
    private boolean runDeleteCart(JSONObject params) throws IOException, JSONException {
        String cart_id = getString(params, B2WConstants.PARAMS_CART_ID);
        Object cartParams[] = { hashPartition(cart_id), cart_id };
        return runAsynchTransaction(Transaction.DELETE_CART, cartParams);      
    }

    // Example JSON
    //
    // {
    //   "operation": "DELETE_CHECKOUT",
    //   "offset": <milliseconds>,
    //   "params": {
    //      "checkoutId": <checkout_id>
    //   }
    // }
    private boolean runDeleteCheckout(JSONObject params) throws IOException, JSONException {
        String checkout_id = getString(params, B2WConstants.PARAMS_CHECKOUT_ID);
        Object checkoutParams[] = { hashPartition(checkout_id), checkout_id };
        return runAsynchTransaction(Transaction.DELETE_CHECKOUT, checkoutParams);
    }
    // Example JSON
    //
    // {
    //   "operation": "GET_STOCK",
    //   "offset": <milliseconds>,
    //   "params": {
    //      "sku": <sku>
    //   }
    // }
    private boolean runGetStock(JSONObject params) throws IOException, JSONException {
        String sku = getString(params, B2WConstants.PARAMS_SKU);
        Object stockParams[] = { hashPartition(sku), sku };
        return runAsynchTransaction(Transaction.GET_STOCK, stockParams);
    }

    // Example JSON
    //
    // {
    //   "operation": "GET_STOCK_QUANTITY",
    //   "offset": <milliseconds>,
    //   "params": {
    //      "stockId": <stock_id>
    //   }
    // }
    private boolean runGetStockQuantity(JSONObject params) throws IOException, JSONException {
        String stock_id = getString(params, B2WConstants.PARAMS_STOCK_ID);
        Object stockParams[] = { hashPartition(stock_id), stock_id };
        return runAsynchTransaction(Transaction.GET_STOCK_QUANTITY, stockParams);
    }

    // Example JSON
    //
    // {
    //   "operation": "GET_STOCK_TRANSACTION",
    //   "offset": <milliseconds>,
    //   "params": {
    //      "transactionId": <transaction_id>
    //   }
    // }
    private boolean runGetStockTransaction(JSONObject params) throws IOException, JSONException {
        String transaction_id = getString(params, B2WConstants.PARAMS_TRANSACTION_ID);
        Object stockParams[] = { hashPartition(transaction_id), transaction_id };
        return runAsynchTransaction(Transaction.GET_STOCK_TRANSACTION, stockParams);
    }

    private boolean runAsynchTransaction(Transaction target, Object params[]) throws IOException {
        if(debug.val) LOG.debug("calling : " + target +  " o:"+target.ordinal() + " : " + target.callName + " params: " + Arrays.asList(params).toString());
        Callback callback = new Callback(target.ordinal());
        return this.getClientHandle().callProcedure(callback, target.callName, params);
    }

    private ClientResponse runSynchTransaction(Transaction target, Object params[]) throws IOException {
        if(debug.val) LOG.debug("calling : " + target +  " o:"+target.ordinal() + " : " + target.callName + " params: " + Arrays.asList(params).toString());
        try {
            ClientResponse clientResponse = this.getClientHandle().callProcedure(target.callName, params);
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, target.ordinal());
            return clientResponse;
        } catch(ProcCallException e) {
            if (debug.val) e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    
    private class Callback implements ProcedureCallback {
        private final int idx;

        public Callback(int idx) {
            this.idx = idx;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, this.idx);

        }
    } // END CLASS
    
    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[B2WProjectBuilder.PROCEDURES.length];
        for (int i = 0; i < procNames.length; i++) {
            procNames[i] = B2WProjectBuilder.PROCEDURES[i].getSimpleName();
        }
        return (procNames);
    }
    
}
