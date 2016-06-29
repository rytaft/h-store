package edu.mit.benchmark.b2w.types;

/** Class containing information about stock transactions in the checkout object. */
public final class CheckoutStockTransaction {
	
	public String line_id;
    public String transaction_id;
    
    public CheckoutStockTransaction() {}
    
	public CheckoutStockTransaction(String line_id, String transaction_id) {
	    this.line_id = line_id;
	    this.transaction_id = transaction_id;
	}


}
