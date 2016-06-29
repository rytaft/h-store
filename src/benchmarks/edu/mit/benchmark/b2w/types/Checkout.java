package edu.mit.benchmark.b2w.types;

/** Class containing information about the checkout object. */
public final class Checkout {
	
    public String cart_id = null; 
    public String deliveryAddressId = null; 
    public String billingAddressId = null; 
    public double amountDue = 0; 
    public double total = 0; 
    public String freightContract = null; 
    public double freightPrice = 0; 
    public String freightStatus = null;
    public CheckoutFreightDelivery[] freightDelivery = null; 
    public CheckoutStockTransaction[] stockTransactions = null;
    
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
