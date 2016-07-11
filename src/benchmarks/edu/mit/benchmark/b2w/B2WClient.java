package edu.mit.benchmark.b2w;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.TimestampType;

import edu.brown.api.BenchmarkComponent;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

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
            Random rand = new Random();
            int key = -1; 
            int scan_count; 
            while (true) {
                runOnce();
            } 
        } 
        catch (IOException e) {
            
        }
    }
    
    
    @Override
    protected boolean runOnce() throws IOException {
        final Operation target = Operation.CHECKOUT; // TODO read operations and params from file
        switch (target) {
            case CHECKOUT:
                return runCheckout();
            case PURCHASE:
                return runPurchase();
            default:
                throw new RuntimeException("Unexpected operation '" + target + "'");
        }
    }
    
    private boolean runCheckout() throws IOException {
        // Get the cart and cart lines
        String cart_id = null;
        Object cartParams[] = { cart_id };
        ClientResponse cartResponse = runSynchTransaction(Transaction.GET_CART, cartParams);
        if (cartResponse.getResults().length != B2WConstants.CART_TABLE_COUNT) return false;        
        final int CART_LINES_RESULTS = 2;
        
        ArrayList<String> line_id_list = new ArrayList<>();
        ArrayList<Integer> requested_quantity_list = new ArrayList<>();
        ArrayList<Integer> reserved_quantity_list = new ArrayList<>();        
        ArrayList<String> status_list = new ArrayList<>();
        ArrayList<String> stock_type_list = new ArrayList<>();
        ArrayList<String> transaction_id_list = new ArrayList<>();

        // Iterate through the cart lines, and attempt to reserve each item.  If the item was successfully reserved,
        // Create a new stock transaction.
        for (int i = 0; i < cartResponse.getResults()[CART_LINES_RESULTS].getRowCount(); ++i) {
            final VoltTableRow cartLine = cartResponse.getResults()[CART_LINES_RESULTS].fetchRow(i);
            final int LINE_ID = 1, QUANTITY = 7;
            String line_id = cartLine.getString(LINE_ID);
            
            // Attempt to reserve the stock
            String stock_id = null; // TODO check a cache based on the sku, otherwise check the db and/or input params
            int requested_quantity = (int) cartLine.getLong(QUANTITY);
            Object reserveStockParams[] = { stock_id, requested_quantity };
            ClientResponse reserveStockResponse = runSynchTransaction(Transaction.RESERVE_STOCK, reserveStockParams);
            if (reserveStockResponse.getResults().length != 1 && 
                    reserveStockResponse.getResults()[0].getRowCount() != 1) return false;
            int reserved_quantity = (int) reserveStockResponse.getResults()[0].fetchRow(0).getLong(0);

            // If successfully reserved, create a stock transaction
            if (reserved_quantity > 0) {
                String transaction_id = null; // get from input params
                String reserve_id = transaction_id; // todo: fixme
                String brand = null;
                TimestampType timestamp = new TimestampType();
                String current_status = B2WConstants.STATUS_NEW;
                TimestampType expiration_date = new TimestampType(new Date(timestamp.getMSTime() + 30 * 60 * 1000)); // add 30 mins
                int is_kit = 0;
                String reserve_lines = null;
                long sku = 0;
                String solr_query = null;
                String status = null;
                long store_id = 0;
                int subinventory = 0;
                int warehouse = 0;

                Object createStockTxnParams[] = new Object[]{ transaction_id, reserve_id, brand, timestamp, current_status,
                        expiration_date, is_kit, requested_quantity, reserve_lines, reserved_quantity, sku, 
                        solr_query, status, store_id, subinventory, warehouse };
                runSynchTransaction(Transaction.CREATE_STOCK_TRANSACTION, createStockTxnParams);

                // store the line id and transaction info to add to the cart and checkout
                String order_status = (requested_quantity == reserved_quantity ? B2WConstants.STATUS_COMPLETED : B2WConstants.STATUS_INCOMPLETE);
                line_id_list.add(line_id);
                requested_quantity_list.add(requested_quantity);
                reserved_quantity_list.add(reserved_quantity);
                status_list.add(order_status);
                stock_type_list.add(null);
                transaction_id_list.add(transaction_id);
            }
        }
        
        // Update the cart with the new transactions, customer, etc
        TimestampType timestamp = new TimestampType();
        String customer_id = null;
        String token = null; 
        int guest = 0;
        int isGuest = 0;
        Object reserveCartParams[] = new Object[]{ cart_id, timestamp, customer_id, token, guest, isGuest, 
                line_id_list.toArray(new String[]{}), requested_quantity_list.toArray(new Integer[]{}), 
                reserved_quantity_list.toArray(new Integer[]{}), status_list.toArray(new String[]{}), 
                stock_type_list.toArray(new String[]{}), transaction_id_list.toArray(new String[]{}) };
        runAsynchTransaction(Transaction.RESERVE_CART, reserveCartParams);

        // Finally, create the checkout object
        String checkout_id = "test";
        String deliveryAddressId = null; 
        String billingAddressId = null;
        double amountDue = 0; 
        double total = 0; 
        String freightContract = null; 
        double freightPrice = 0; 
        String freightStatus = null;
        String line_id[] = new String[]{}; 
        int delivery_time[] = new int[]{};
        Object checkoutParams[] = new Object[]{ checkout_id, cart_id, deliveryAddressId, billingAddressId, amountDue, total, 
                freightContract, freightPrice, freightStatus, line_id, transaction_id_list.toArray(new String[]{}), delivery_time };
        
        return runAsynchTransaction(Transaction.CREATE_CHECKOUT, checkoutParams);   
    }
    
    private boolean runPurchase() throws IOException {
        // Add payment info to the checkout object
        String checkout_id = null;
        String cart_id = null;
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
