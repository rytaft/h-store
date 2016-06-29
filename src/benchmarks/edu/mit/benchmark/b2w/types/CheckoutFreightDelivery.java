package edu.mit.benchmark.b2w.types;

/** Class containing information about freight delivery in the checkout object. */
public final class CheckoutFreightDelivery {
	
	public String line_id;
    public int delivery_time;
    
    public CheckoutFreightDelivery() {}
    
	public CheckoutFreightDelivery(String line_id, int delivery_time) {
	    this.line_id = line_id;
	    this.delivery_time = delivery_time;
	}


}
