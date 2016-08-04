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
        CHECKOUT,
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
            case ADD_LINE_TO_CART:
                return runAddLineToCart(params);
            case CHECKOUT:
                return runCheckout(params);
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
    //   }
    // }
    private boolean runAddLineToCart(JSONObject params) throws IOException, JSONException {
        String cart_id = params.getString(B2WConstants.PARAMS_CART_ID);
        TimestampType timestamp = new TimestampType(params.getLong(B2WConstants.PARAMS_TIMESTAMP));
        String line_id = params.getString(B2WConstants.PARAMS_LINE_ID);
        long product_sku = params.getLong(B2WConstants.PARAMS_PRODUCT_SKU); 
        long product_id = params.getLong(B2WConstants.PARAMS_PRODUCT_ID);
        long store_id = params.getLong(B2WConstants.PARAMS_STORE_ID);
        int quantity = params.getInt(B2WConstants.PARAMS_QUANTITY);
        String salesChannel = params.getString(B2WConstants.PARAMS_SALES_CHANNEL);
        String opn = params.getString(B2WConstants.PARAMS_OPN);
        String epar = params.getString(B2WConstants.PARAMS_EPAR);
        int autoMerge = params.getInt(B2WConstants.PARAMS_AUTO_MERGE);        
        double unitSalesPrice = params.getDouble(B2WConstants.PARAMS_UNIT_SALES_PRICE);
        double salesPrice = params.getDouble(B2WConstants.PARAMS_SALES_PRICE);
        int maxQuantity = params.getInt(B2WConstants.PARAMS_MAX_QUANTITY);
        String maximumQuantityReason = params.getString(B2WConstants.PARAMS_MAXIMUM_QUANTITY_REASON);
        String type = params.getString(B2WConstants.PARAMS_TYPE);
        String stockTransactionId = params.getString(B2WConstants.PARAMS_STOCK_TRANSACTION_ID);
        int requestedQuantity = params.getInt(B2WConstants.PARAMS_REQUESTED_QUANTITY);
        String line_status = params.getString(B2WConstants.PARAMS_LINE_STATUS);
        String stockType = params.getString(B2WConstants.PARAMS_STOCK_TYPE);
        String image = params.getString(B2WConstants.PARAMS_IMAGE);
        String name = params.getString(B2WConstants.PARAMS_NAME);
        int isKit = params.getInt(B2WConstants.PARAMS_IS_KIT);
        double price = params.getDouble(B2WConstants.PARAMS_PRICE);
        double originalPrice = params.getDouble(B2WConstants.PARAMS_ORIGINAL_PRICE);
        int isLarge = params.getInt(B2WConstants.PARAMS_IS_LARGE);
        long department = params.getLong(B2WConstants.PARAMS_DEPARTMENT);
        long line = params.getLong(B2WConstants.PARAMS_LINE);
        long subClass = params.getLong(B2WConstants.PARAMS_SUB_CLASS);
        double weight = params.getDouble(B2WConstants.PARAMS_WEIGHT);
        long product_class = params.getLong(B2WConstants.PARAMS_PRODUCT_CLASS);
        
        // TODO: What if there is already a checkout object?  Should we try to reserve this item here, and update the cart/checkout?
        
        Object addLineParams[] = { cart_id, timestamp, line_id, 
                product_sku, product_id, store_id, quantity, salesChannel, opn, epar, autoMerge,
                unitSalesPrice, salesPrice, maxQuantity, maximumQuantityReason, type, stockTransactionId,
                requestedQuantity, line_status, stockType, image, name, isKit, price, originalPrice,
                isLarge, department, line, subClass, weight, product_class };
        return runAsynchTransaction(Transaction.ADD_LINE_TO_CART, addLineParams);   
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
    //          "solrQuery": <solr_query>, 
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
            int requested_quantity = (int) cartLine.getLong(QUANTITY);
            int total_reserved_quantity = 0;
            String transaction_id = line.getString(B2WConstants.PARAMS_TRANSACTION_ID); 
            if (transaction_id == null || transaction_id.isEmpty()) {
                if (debug.val) LOG.debug("No transaction_id for line_id: " + line_id);
            }
            
            // TODO we should probably check a cache based on the sku to find the stock_ids, and every 5 minutes
            // refresh the cache by running the GetStock transaction
            
            // This code currently tries to reserve stock_ids that we know succeeded from the input parameters
            
            JSONArray reserves = params.getJSONArray(B2WConstants.PARAMS_RESERVES);
            for (int j = 0; j < reserves.length(); ++j) {
                JSONObject reserve = reserves.getJSONObject(j);
                String stock_id = reserve.getString(B2WConstants.PARAMS_STOCK_ID); 
                if (requested_quantity != reserve.getInt(B2WConstants.PARAMS_REQUESTED_QUANTITY)) {
                    LOG.info("Requested quantity in database <" + requested_quantity + "> doesn't match log <" + reserve.getInt(B2WConstants.PARAMS_REQUESTED_QUANTITY) +">");
                }
             
                // Attempt to reserve the stock
                Object reserveStockParams[] = { stock_id, requested_quantity };
                ClientResponse reserveStockResponse = runSynchTransaction(Transaction.RESERVE_STOCK, reserveStockParams);
                if (reserveStockResponse.getResults().length != 1 && 
                        reserveStockResponse.getResults()[0].getRowCount() != 1) return false;
                int reserved_quantity = (int) reserveStockResponse.getResults()[0].fetchRow(0).getLong(0);
                if (reserved_quantity != reserve.getInt(B2WConstants.PARAMS_RESERVED_QUANTITY)) {
                    LOG.info("Reserved quantity in database <" + reserved_quantity + "> doesn't match log <" + reserve.getInt(B2WConstants.PARAMS_RESERVED_QUANTITY) +">");
                }

                // If successfully reserved, create a stock transaction
                if (reserved_quantity > 0 && transaction_id != null && !transaction_id.isEmpty()) {
                    String reserve_id = reserve.getString(B2WConstants.PARAMS_RESERVE_ID); 
                    String brand = reserve.getString(B2WConstants.PARAMS_BRAND);
                    TimestampType timestamp = new TimestampType(reserve.getLong(B2WConstants.PARAMS_CREATION_DATE));
                    if (timestamp.getMSTime() > cartTimestamp.getMSTime()) cartTimestamp = timestamp;
                    String current_status = B2WConstants.STATUS_NEW;
                    TimestampType expiration_date = new TimestampType(new Date(timestamp.getMSTime() + 30 * 60 * 1000)); // add 30 mins
                    int is_kit = reserve.getInt(B2WConstants.PARAMS_IS_KIT);
                    String reserve_lines = reserve.getString(B2WConstants.PARAMS_RESERVE_LINES);
                    long sku = reserve.getLong(B2WConstants.PARAMS_SKU);
                    String solr_query = reserve.getString(B2WConstants.PARAMS_SOLR_QUERY);
                    JSONObject status_obj = new JSONObject();
                    status_obj.append(timestamp.toString(), current_status);
                    String status = status_obj.toString();
                    long store_id = reserve.getLong(B2WConstants.PARAMS_STORE_ID);
                    int subinventory = reserve.getInt(B2WConstants.PARAMS_SUBINVENTORY);
                    int warehouse = reserve.getInt(B2WConstants.PARAMS_WAREHOUSE);

                    // TODO should this be a single transaction if there are multiple reserves?
                    Object createStockTxnParams[] = new Object[]{ transaction_id, reserve_id, brand, timestamp, current_status,
                            expiration_date, is_kit, requested_quantity, reserve_lines, reserved_quantity, sku, 
                            solr_query, status, store_id, subinventory, warehouse };
                    runSynchTransaction(Transaction.CREATE_STOCK_TRANSACTION, createStockTxnParams);

                    total_reserved_quantity += reserved_quantity;
                }
            }
            
            // store the line id and transaction info to add to the cart and checkout
            line_ids[i] = line_id;
            requested_quantities[i] = requested_quantity;
            reserved_quantities[i] = total_reserved_quantity;
            statuses[i] = (requested_quantity == total_reserved_quantity ? B2WConstants.STATUS_COMPLETED : B2WConstants.STATUS_INCOMPLETE);
            stock_types[i] = line.getString(B2WConstants.PARAMS_STOCK_TYPE);
            transaction_ids[i] = transaction_id;
            delivery_times[i] = line.getInt(B2WConstants.PARAMS_DELIVERY_TIME);
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
        String checkout_id = params.getString(B2WConstants.PARAMS_CHECKOUT_ID);
        String cart_id = params.getString(B2WConstants.PARAMS_CART_ID);
        String paymentOptionId = params.getString(B2WConstants.PARAMS_PAYMENT_OPTION_ID);
        String paymentOptionType = params.getString(B2WConstants.PARAMS_PAYMENT_OPTION_TYPE);
        int dueDays = params.getInt(B2WConstants.PARAMS_DUE_DAYS);
        double amount = params.getDouble(B2WConstants.PARAMS_AMOUNT);
        int installmentQuantity = params.getInt(B2WConstants.PARAMS_INSTALLMENT_QUANTITY);
        double interestAmount = params.getDouble(B2WConstants.PARAMS_INTEREST_AMOUNT);
        int interestRate = params.getInt(B2WConstants.PARAMS_INTEREST_RATE);
        int annualCET = params.getInt(B2WConstants.PARAMS_ANNUAL_CET);
        String number = params.getString(B2WConstants.PARAMS_NUMBER);
        long criptoNumber = params.getLong(B2WConstants.PARAMS_CRIPTO_NUMBER);
        String holdersName = params.getString(B2WConstants.PARAMS_HOLDERS_NAME);
        long securityCode = params.getLong(B2WConstants.PARAMS_SECURITY_CODE);
        String expirationDate = params.getString(B2WConstants.PARAMS_EXPIRATION_DATE);

        Object checkoutPaymentParams[] = { checkout_id, cart_id, paymentOptionId, paymentOptionType, dueDays, amount, installmentQuantity,
                interestAmount, interestRate, annualCET, number, criptoNumber, holdersName, securityCode, expirationDate };
        runSynchTransaction(Transaction.CREATE_CHECKOUT_PAYMENT, checkoutPaymentParams);
        
        // Get all the stock transactions for the purchase from the checkout object
        Object checkoutParams[] = { checkout_id };
        ClientResponse checkoutResponse = runSynchTransaction(Transaction.GET_CHECKOUT, checkoutParams);
        if (checkoutResponse.getResults().length != B2WConstants.CHECKOUT_TABLE_COUNT) return false;        
        final int CHECKOUT_STOCK_TRANSACTIONS_RESULTS = 3;
        
        TimestampType timestamp = new TimestampType(params.getLong(B2WConstants.PARAMS_TIMESTAMP));
        boolean success = true;
        for (int i = 0; i < checkoutResponse.getResults()[CHECKOUT_STOCK_TRANSACTIONS_RESULTS].getRowCount(); ++i) {
            final VoltTableRow checkoutStockTransaction = checkoutResponse.getResults()[CHECKOUT_STOCK_TRANSACTIONS_RESULTS].fetchRow(i);
            final int TRANSACTION_ID = 1;
            String transaction_id = checkoutStockTransaction.getString(TRANSACTION_ID);
            
            Object getStockTxnParams[] = { transaction_id };
            ClientResponse getStockTxnResponse = runSynchTransaction(Transaction.GET_STOCK_TRANSACTION, getStockTxnParams);
            if (getStockTxnResponse.getResults().length != 1) return false;
            for (int j = 0; j < getStockTxnResponse.getResults()[0].getRowCount(); ++j) {
                final VoltTableRow stockTransaction = getStockTxnResponse.getResults()[0].fetchRow(j);
                final int CURRENT_STATUS = 4, RESERVE_LINES = 8;
                
                String current_status = stockTransaction.getString(CURRENT_STATUS);                
                if (current_status.equals(B2WConstants.STATUS_CANCELLED)) {
                    // TODO: try to reserve stock again
                } else {
                    String reserve_lines = stockTransaction.getString(RESERVE_LINES);
                    JSONObject reserve_lines_obj = new JSONObject(reserve_lines);
                    String stock_id = reserve_lines_obj.getString(B2WConstants.PARAMS_STOCK_ID);
                    int reserved_quantity = reserve_lines_obj.getInt(B2WConstants.PARAMS_RESERVED_QUANTITY);
                    Object purchaseStockParams[] = { stock_id, reserved_quantity };
                    success = runAsynchTransaction(Transaction.PURCHASE_STOCK, purchaseStockParams);
                } 
                
                if (!success) return false;
            }
            
            String current_status = B2WConstants.STATUS_PURCHASED;
            Object updateStockTxnParams[] = { transaction_id, timestamp, current_status };
            success = runAsynchTransaction(Transaction.UPDATE_STOCK_TRANSACTION, updateStockTxnParams);   
            
            if (!success) return false;
        }
        
        return success;
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
        String cart_id = params.getString(B2WConstants.PARAMS_CART_ID);
        Object cartParams[] = { cart_id };
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
        String checkout_id = params.getString(B2WConstants.PARAMS_CHECKOUT_ID);
        Object checkoutParams[] = { checkout_id };
        return runAsynchTransaction(Transaction.GET_CHECKOUT, checkoutParams);
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
        long sku = params.getLong(B2WConstants.PARAMS_SKU);
        Object stockParams[] = { sku };
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
        String stock_id = params.getString(B2WConstants.PARAMS_STOCK_ID);
        Object stockParams[] = { stock_id };
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
        String transaction_id = params.getString(B2WConstants.PARAMS_TRANSACTION_ID);
        Object stockParams[] = { transaction_id };
        return runAsynchTransaction(Transaction.GET_STOCK_TRANSACTION, stockParams);
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
