package edu.mit.benchmark.b2w_sku_key;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class LoaderSelector {

	String filename;
	BufferedReader br = null;
	
    private static LoaderSelector cartSelector = null;
    private static LoaderSelector cartCustomerSelector = null;
    private static LoaderSelector cartLineProductsSelector = null;
    private static LoaderSelector cartLineProductStoresSelector = null;
    private static LoaderSelector cartLineProductWarrantiesSelector = null;
    private static LoaderSelector cartLinePromotionsSelector = null;
    private static LoaderSelector cartLinesSelector = null;
    private static LoaderSelector checkoutSelector = null;
    private static LoaderSelector checkoutFreightDeliveryTimeSelector = null;
    private static LoaderSelector checkoutPaymentsSelector = null;
    private static LoaderSelector checkoutStockTransactionsSelector = null;
    private static LoaderSelector stkInventoryStockSelector = null;
    private static LoaderSelector stkInventoryStockQuantitySelector = null;
    private static LoaderSelector stkStockTransactionSelector = null;


    public static synchronized LoaderSelector getLoaderSelector(String filename, B2WConfig config) throws FileNotFoundException {
        if (filename.equals(config.STK_INVENTORY_STOCK_DATA_FILE)) {
            if (stkInventoryStockSelector == null) stkInventoryStockSelector = new LoaderSelector(filename);
            return stkInventoryStockSelector;
        }
        if (filename.equals(config.STK_INVENTORY_STOCK_QUANTITY_DATA_FILE)) {
            if (stkInventoryStockQuantitySelector == null) stkInventoryStockQuantitySelector = new LoaderSelector(filename);
            return stkInventoryStockQuantitySelector;
        }
        if (filename.equals(config.STK_STOCK_TRANSACTION_DATA_FILE)) {
            if (stkStockTransactionSelector == null) stkStockTransactionSelector = new LoaderSelector(filename);
            return stkStockTransactionSelector;
        }
        if (filename.equals(config.CART_DATA_FILE)) {
           if (cartSelector == null) cartSelector = new LoaderSelector(filename);
           return cartSelector;
        }
        if (filename.equals(config.CART_CUSTOMER_DATA_FILE)) {
           if (cartCustomerSelector == null) cartCustomerSelector = new LoaderSelector(filename);
           return cartCustomerSelector;
        }
        if (filename.equals(config.CART_LINES_DATA_FILE)) {
           if (cartLinesSelector == null) cartLinesSelector = new LoaderSelector(filename);
           return cartLinesSelector;
        }
        if (filename.equals(config.CART_LINE_PRODUCTS_DATA_FILE)) {
           if (cartLineProductsSelector == null) cartLineProductsSelector = new LoaderSelector(filename);
           return cartLineProductsSelector;
        }
        if (filename.equals(config.CART_LINE_PROMOTIONS_DATA_FILE)) {
           if (cartLinePromotionsSelector == null) cartLinePromotionsSelector = new LoaderSelector(filename);
           return cartLinePromotionsSelector;
        }
        if (filename.equals(config.CART_LINE_PRODUCT_WARRANTIES_DATA_FILE)) {
           if (cartLineProductWarrantiesSelector == null) cartLineProductWarrantiesSelector = new LoaderSelector(filename);
           return cartLineProductWarrantiesSelector;
        }
        if (filename.equals(config.CART_LINE_PRODUCT_STORES_DATA_FILE)) {
           if (cartLineProductStoresSelector == null) cartLineProductStoresSelector = new LoaderSelector(filename);
           return cartLineProductStoresSelector;
        }
        if (filename.equals(config.CHECKOUT_DATA_FILE)) {
           if (checkoutSelector == null) checkoutSelector = new LoaderSelector(filename);
           return checkoutSelector;
        }
        if (filename.equals(config.CHECKOUT_PAYMENTS_DATA_FILE)) {
           if (checkoutPaymentsSelector == null) checkoutPaymentsSelector = new LoaderSelector(filename);
           return checkoutPaymentsSelector;
        }
        if (filename.equals(config.CHECKOUT_FREIGHT_DELIVERY_TIME_DATA_FILE)) {
           if (checkoutFreightDeliveryTimeSelector == null) checkoutFreightDeliveryTimeSelector = new LoaderSelector(filename);
           return checkoutFreightDeliveryTimeSelector;
        }
        if (filename.equals(config.CHECKOUT_STOCK_TRANSACTIONS_DATA_FILE)) {
           if (checkoutStockTransactionsSelector == null) checkoutStockTransactionsSelector = new LoaderSelector(filename);
           return checkoutStockTransactionsSelector;
        }
        else {
            throw new FileNotFoundException("File " + filename + " not recognized");
        }
    }
    
    public static synchronized void closeAll() throws IOException {
        if (stkInventoryStockSelector != null) {
            stkInventoryStockSelector.close();
            stkInventoryStockSelector = null;
        }
        if (stkInventoryStockQuantitySelector != null) {
            stkInventoryStockQuantitySelector.close();
            stkInventoryStockQuantitySelector = null;
        }
        if (stkStockTransactionSelector != null) {
            stkStockTransactionSelector.close();
            stkStockTransactionSelector = null;
        }
        if (cartSelector != null) {
            cartSelector.close();
            cartSelector = null;
        }
        if (cartCustomerSelector != null) {
            cartCustomerSelector.close();
            cartCustomerSelector = null;
        }
        if (cartLinesSelector != null) {
            cartLinesSelector.close();
            cartLinesSelector = null;
        }
        if (cartLineProductsSelector != null) {
            cartLineProductsSelector.close();
            cartLineProductsSelector = null;
        }
        if (cartLinePromotionsSelector != null) {
            cartLinePromotionsSelector.close();
            cartLinePromotionsSelector = null;
        }
        if (cartLineProductWarrantiesSelector != null) {
            cartLineProductWarrantiesSelector.close();
            cartLineProductWarrantiesSelector = null;
        }
        if (cartLineProductStoresSelector != null) {
            cartLineProductStoresSelector.close();
            cartLineProductStoresSelector = null;
        }
        if (checkoutSelector != null) {
            checkoutSelector.close();
            checkoutSelector = null;
        }
        if (checkoutPaymentsSelector != null) {
            checkoutPaymentsSelector.close();
            checkoutPaymentsSelector = null;
        }
        if (checkoutFreightDeliveryTimeSelector != null) {
            checkoutFreightDeliveryTimeSelector.close();
            checkoutFreightDeliveryTimeSelector = null;
        }
        if (checkoutStockTransactionsSelector != null) {
            checkoutStockTransactionsSelector.close();
            checkoutStockTransactionsSelector = null;
        }
    }

    public String getFilename(){
        return filename;
    }

	private LoaderSelector(String filename) throws FileNotFoundException {
		this.filename = filename;

		if(filename==null || filename.isEmpty())
			throw new FileNotFoundException("You must specify a filename to instantiate the LoadSelector... (probably missing in your workload configuration?)");

        File file = new File(filename);
        FileReader fr = new FileReader(file);
        br = new BufferedReader(fr);
	}

	public synchronized String nextLine() throws IOException {
		return readNextLine();
	}

	private String readNextLine() throws IOException {
		return br.readLine();
	}

	public ArrayList<String> readAll() throws IOException {
		ArrayList<String> lines = new ArrayList<String>();

		while (true) {
		    String line = readNextLine();
		    if(line == null) break;
		    lines.add(line);
		}

		return lines;
	}

	public void close() throws IOException {
		br.close();
	}

}
