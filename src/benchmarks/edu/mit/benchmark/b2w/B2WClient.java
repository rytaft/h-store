package edu.mit.benchmark.b2w;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.VoltTableRow;
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

public class B2WClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(B2WClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }

    public static enum Transaction {
        ADD_LINE_TO_CART("AddLineToCart"),
        CREATE_CHECKOUT("CreateCheckout"),
        CREATE_CHECKOUT_PAYMENT("CreateCheckoutPayment"),
        CREATE_STOCK_TRANSACTION("CreateStockTransaction"),
        DELETE_LINE_FROM_CART("DeleteLineFromCart"),
        GET_CART("GetCart"),
        GET_CHECKOUT("GetCheckout"),
        GET_STOCK("GetStock"),
        GET_STOCK_QUANTITY("GetStockQuantity"),
        PURCHASE_STOCK("PurchaseStock"),
        RESERVE_CART("ReserveCart"),
        RESERVE_STOCK("ReserveStock");
        
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
        CHECKOUT,
        PURCHASE;
    
    } // OPERATION ENUM

    
    
    public static void main(String args[]) {
        BenchmarkComponent.main(B2WClient.class, args, false);
    }


    private B2WConfig config;
    private TransactionSelector txn_selector;

    /**
     *  Time of first transaction in milliseconds
     */
    private long startTime;
  

    public B2WClient(String[] args) {
        super(args);     
        this.config = new B2WConfig(m_extraParams);

    }

    @SuppressWarnings("unused")
    @Deprecated
    @Override
    public void runLoop() {

        try {
            Client client = this.getClientHandle();

            startTime = System.currentTimeMillis();
            final boolean doSingle = this.getHStoreConf().client.runOnce;
            boolean hadErrors = false;
            boolean bp = false;
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

                long offset = 0;
                JSONObject next_txn = null;
                try {
                    next_txn = txn_selector.nextTransaction();
                    offset = next_txn.getLong(B2WConstants.OPERATION_OFFSET);
                } catch (JSONException e) {
                    LOG.error("Failed to parse transaction: " + e.getMessage(), e);
                }

                // Wait to generate the transaction until sufficient time has passed
                final long now = System.currentTimeMillis();
                final long delta = now - startTime;
                if (delta >= offset) {
                    try {
                        bp = !this.runOnce(next_txn);

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
        switch (operation) {
            case CHECKOUT:
                return runCheckout(params);
            case PURCHASE:
                return runPurchase(params);
            default:
                throw new RuntimeException("Unexpected operation '" + operation + "'");
        }
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
    //        "stockId": <stock_id>, 
    //        "transactionId": <transaction_id>, 
    //        "reserveId": <reserve_id>, 
    //        "brand": <brand>, 
    //        "stockTxnCreationTimestamp": <creation_date>, 
    //        "isKit": <is_kit>, 
    //        "requestedQuantity": <requested_quantity>, 
    //        "reserveLines": <reserve_lines>, 
    //        "reservedQuantity": <reserved_quantity>, 
    //        "sku": <sku>, 
    //        "solrQuery": <solr_query>, 
    //        "storeId": <store_id>, 
    //        "subinventory": <subinventory>, 
    //        "warehouse": <warehouse>,
    //        "stockType": <stock_type>,
    //        "deliveryTime": <delivery_time>
    //      },{
    //        ... 
    //      }]
    //   }
    // }
    private boolean runCheckout(JSONObject params) throws IOException, JSONException {
        // Get the cart and cart lines
        String cart_id = params.getString(B2WConstants.PARAMS_CART_ID);
        Object cartParams[] = { cart_id };
        ClientResponse cartResponse = runSynchTransaction(Transaction.GET_CART, cartParams);
        if (cartResponse.getResults().length != B2WConstants.CART_TABLE_COUNT) return false;        
        final int CART_LINES_RESULTS = 2;
        
        JSONArray lines = params.getJSONArray(B2WConstants.PARAMS_LINES);
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
            final int LINE_ID = 1, QUANTITY = 7;
            String line_id = cartLine.getString(LINE_ID);
            if (!lines_map.containsKey(line_id)) {
                LOG.info("No log info for line_id: " + line_id);
                continue;
            }
            JSONObject line = lines_map.get(line_id);
            
            // Attempt to reserve the stock
            String stock_id = line.getString(B2WConstants.PARAMS_STOCK_ID); // TODO check a cache based on the sku, otherwise check the db?
            int requested_quantity = (int) cartLine.getLong(QUANTITY);
            if (requested_quantity != line.getInt(B2WConstants.PARAMS_REQUESTED_QUANTITY)) {
                LOG.info("Requested quantity in database <" + requested_quantity + "> doesn't match log <" + line.getInt(B2WConstants.PARAMS_REQUESTED_QUANTITY) +">");
            }
            Object reserveStockParams[] = { stock_id, requested_quantity };
            ClientResponse reserveStockResponse = runSynchTransaction(Transaction.RESERVE_STOCK, reserveStockParams);
            if (reserveStockResponse.getResults().length != 1 && 
                    reserveStockResponse.getResults()[0].getRowCount() != 1) return false;
            int reserved_quantity = (int) reserveStockResponse.getResults()[0].fetchRow(0).getLong(0);
            if (reserved_quantity != line.getInt(B2WConstants.PARAMS_RESERVED_QUANTITY)) {
                LOG.info("Reserved quantity in database <" + reserved_quantity + "> doesn't match log <" + line.getInt(B2WConstants.PARAMS_RESERVED_QUANTITY) +">");
            }

            // If successfully reserved, create a stock transaction
            if (reserved_quantity > 0) {
                String transaction_id = line.getString(B2WConstants.PARAMS_TRANSACTION_ID); 
                if (transaction_id == null || transaction_id.isEmpty()) {
                    LOG.info("No transaction_id for line_id: " + line_id);
                    continue;
                }
                String reserve_id = line.getString(B2WConstants.PARAMS_RESERVE_ID); 
                String brand = line.getString(B2WConstants.PARAMS_BRAND);
                TimestampType timestamp = new TimestampType(line.getLong(B2WConstants.PARAMS_CREATION_DATE));
                if (timestamp.getMSTime() > cartTimestamp.getMSTime()) cartTimestamp = timestamp;
                String current_status = B2WConstants.STATUS_NEW;
                TimestampType expiration_date = new TimestampType(new Date(timestamp.getMSTime() + 30 * 60 * 1000)); // add 30 mins
                int is_kit = line.getInt(B2WConstants.PARAMS_IS_KIT);
                String reserve_lines = line.getString(B2WConstants.PARAMS_RESERVE_LINES);
                long sku = line.getLong(B2WConstants.PARAMS_SKU);
                String solr_query = line.getString(B2WConstants.PARAMS_SOLR_QUERY);
                JSONObject status_obj = new JSONObject();
                status_obj.append(timestamp.toString(), current_status);
                String status = status_obj.toString();
                long store_id = line.getLong(B2WConstants.PARAMS_STORE_ID);
                int subinventory = line.getInt(B2WConstants.PARAMS_SUBINVENTORY);
                int warehouse = line.getInt(B2WConstants.PARAMS_WAREHOUSE);

                Object createStockTxnParams[] = new Object[]{ transaction_id, reserve_id, brand, timestamp, current_status,
                        expiration_date, is_kit, requested_quantity, reserve_lines, reserved_quantity, sku, 
                        solr_query, status, store_id, subinventory, warehouse };
                runSynchTransaction(Transaction.CREATE_STOCK_TRANSACTION, createStockTxnParams);

                // store the line id and transaction info to add to the cart and checkout
                String order_status = (requested_quantity == reserved_quantity ? B2WConstants.STATUS_COMPLETED : B2WConstants.STATUS_INCOMPLETE);
                line_ids[i] = line_id;
                requested_quantities[i] = requested_quantity;
                reserved_quantities[i] = reserved_quantity;
                statuses[i] = order_status;
                stock_types[i] = line.getString(B2WConstants.PARAMS_STOCK_TYPE);
                transaction_ids[i] = transaction_id;
                delivery_times[i] = line.getInt(B2WConstants.PARAMS_DELIVERY_TIME);
            }
        }
        
        // Update the cart with the new transactions, customer, etc
        String customer_id = params.getString(B2WConstants.PARAMS_CUSTOMER_ID);
        String token = params.getString(B2WConstants.PARAMS_TOKEN); 
        int guest = params.getInt(B2WConstants.PARAMS_GUEST);
        int isGuest = params.getInt(B2WConstants.PARAMS_IS_GUEST);
        Object reserveCartParams[] = new Object[]{ cart_id, cartTimestamp, customer_id, token, guest, isGuest, 
                line_ids, requested_quantities, reserved_quantities, statuses, stock_types, transaction_ids };
        runAsynchTransaction(Transaction.RESERVE_CART, reserveCartParams);

        // Finally, create the checkout object
        String checkout_id = params.getString(B2WConstants.PARAMS_CHECKOUT_ID);
        String deliveryAddressId = params.getString(B2WConstants.PARAMS_DELIVERY_ADDRESS_ID); 
        String billingAddressId = params.getString(B2WConstants.PARAMS_BILLING_ADDRESS_ID);
        double amountDue = params.getDouble(B2WConstants.PARAMS_AMOUNT_DUE); 
        double total = params.getDouble(B2WConstants.PARAMS_TOTAL); 
        String freightContract = params.getString(B2WConstants.PARAMS_FREIGHT_CONTRACT); 
        double freightPrice = params.getDouble(B2WConstants.PARAMS_FREIGHT_PRICE); 
        String freightStatus = params.getString(B2WConstants.PARAMS_FREIGHT_STATUS);
        Object checkoutParams[] = new Object[]{ checkout_id, cart_id, deliveryAddressId, billingAddressId, amountDue, total, 
                freightContract, freightPrice, freightStatus, line_ids, transaction_ids, delivery_times };
        
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
    //      "lines": [{
    //        "lineId": <line_id>,
    //        "stockId": <stock_id>, 
    //        "transactionId": <transaction_id>, 
    //        "reserveId": <reserve_id>, 
    //        "brand": <brand>, 
    //        "stockTxnCreationTimestamp": <creation_date>, 
    //        "isKit": <is_kit>, 
    //        "requestedQuantity": <requested_quantity>, 
    //        "reserveLines": <reserve_lines>, 
    //        "reservedQuantity": <reserved_quantity>, 
    //        "sku": <sku>, 
    //        "solrQuery": <solr_query>, 
    //        "storeId": <store_id>, 
    //        "subinventory": <subinventory>, 
    //        "warehouse": <warehouse>,
    //        "stockType": <stock_type>,
    //        "deliveryTime": <delivery_time>
    //      },{
    //        ... 
    //      }]
    //   }
    // }
    private boolean runPurchase(JSONObject params) throws IOException, JSONException {
        // Add payment info to the checkout object
        String checkout_id = params.getString(B2WConstants.PARAMS_CHECKOUT_ID);
        String cart_id = params.getString(B2WConstants.PARAMS_CART_ID);
        Object checkoutPaymentParams[] = { checkout_id, cart_id };
        runSynchTransaction(Transaction.CREATE_CHECKOUT_PAYMENT, checkoutPaymentParams);
        
        // Get all the stock transactions for the purchase from the checkout object
        Object checkoutParams[] = { checkout_id };
        ClientResponse checkoutResponse = runSynchTransaction(Transaction.GET_CHECKOUT, checkoutParams);
        if (checkoutResponse.getResults().length != B2WConstants.CHECKOUT_TABLE_COUNT) return false;        
        final int CHECKOUT_STOCK_TRANSACTIONS_RESULTS = 3;
        
        for (int i = 0; i < checkoutResponse.getResults()[CHECKOUT_STOCK_TRANSACTIONS_RESULTS].getRowCount(); ++i) {
            final VoltTableRow stockTransaction = checkoutResponse.getResults()[CHECKOUT_STOCK_TRANSACTIONS_RESULTS].fetchRow(i);
            
            // TODO do something with the stockTransaction (purchaseStock)
        }
        
        return true; // TODO return the result of an asynchTransaction
    }
    
    private boolean runAsynchTransaction(Transaction target, Object params[]) throws IOException {
        if(debug.val) LOG.debug("calling : " + target +  " o:"+target.ordinal() + " : " + target.callName);
        Callback callback = new Callback(target.ordinal());
        return this.getClientHandle().callProcedure(callback, target.callName, params);
    }

    private ClientResponse runSynchTransaction(Transaction target, Object params[]) throws IOException {
        if(debug.val) LOG.debug("calling : " + target +  " o:"+target.ordinal() + " : " + target.callName);
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
