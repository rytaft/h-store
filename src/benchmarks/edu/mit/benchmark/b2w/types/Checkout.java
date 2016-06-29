package edu.mit.benchmark.b2w.types;

/** Class containing information about the checkout object. */
public final class Checkout {
	
    public String cart_id; 
    public String deliveryAddressId; 
    public String billingAddressId; 
    public double amountDue; 
    public double total; 
    public String freightContract; 
    public double freightPrice; 
    public String freightStatus;
    public CheckoutFreightDelivery[] freightDelivery; 
    public CheckoutStockTransaction[] stockTransactions;
    
    public Checkout() {}
    
	public Checkout(String checkout_id, String cart_id, String deliveryAddressId, String billingAddressId, 
            double amountDue, double total, String freightContract, double freightPrice, String freightStatus,
            CheckoutFreightDelivery[] freightDelivery, CheckoutStockTransaction[] stockTransactions) {
	    this.cart_id = cart_id; 
	    this.deliveryAddressId = deliveryAddressId; 
	    this.billingAddressId = billingAddressId; 
	    this.amountDue = amountDue; 
	    this.total = total; 
	    this.freightContract = freightContract; 
	    this.freightPrice = freightPrice; 
	    this.freightStatus = freightStatus;
	}


}
