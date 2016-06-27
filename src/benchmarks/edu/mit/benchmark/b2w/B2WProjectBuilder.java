package edu.mit.benchmark.b2w;

import org.voltdb.VoltProcedure;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.mit.benchmark.b2w.procedures.AddCustomerToCart;
import edu.mit.benchmark.b2w.procedures.AddLineToCart;
import edu.mit.benchmark.b2w.procedures.CreateCheckout;
import edu.mit.benchmark.b2w.procedures.CreateCheckoutPayment;
import edu.mit.benchmark.b2w.procedures.DeleteLineFromCart;
import edu.mit.benchmark.b2w.procedures.GetCart;
import edu.mit.benchmark.b2w.procedures.GetCheckout;
import edu.mit.benchmark.b2w.procedures.GetStock;
import edu.mit.benchmark.b2w.procedures.GetStockQuantity;
import edu.mit.benchmark.b2w.procedures.PurchaseStock;
import edu.mit.benchmark.b2w.procedures.ReserveStock;

public class B2WProjectBuilder extends AbstractProjectBuilder{

    
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = B2WClient.class;
 
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = B2WLoader.class;
 
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[]) new Class<?>[] {
        AddCustomerToCart.class,
        AddLineToCart.class,
        CreateCheckout.class,
        CreateCheckoutPayment.class,
        DeleteLineFromCart.class,
        GetCart.class,
        GetCheckout.class,
        GetStock.class,
        GetStockQuantity.class,
        PurchaseStock.class,
        ReserveStock.class
    };
    public static final String PARTITIONING[][] = new String[][] {
        // { "TABLE NAME", "PARTITIONING COLUMN NAME" }
        {"STK_INVENTORY_STOCK", "sku"},
        {"STK_INVENTORY_STOCK_QUANTITY", "id"},
        {"STK_STOCK_TRANSACTION", "transaction_id"},
        {"CART", "id"},
        {"CART_CUSTOMER", "cartId"},
        {"CART_LINES", "cartId"},
        {"CART_LINE_PRODUCTS", "cartId"},
        {"CART_LINE_PROMOTIONS", "cartId"},
        {"CART_LINE_PRODUCT_WARRANTIES", "cartId"},
        {"CART_LINE_PRODUCT_STORES", "cartId"},
        {"CHECKOUT", "id"},
        {"CHECKOUT_PAYMENTS", "checkoutId"},
        {"CHECKOUT_FREIGHT_DELIVERY_TIME", "checkoutId"},
        {"CHECKOUT_STOCK_TRANSACTIONS", "checkoutId"},
    };
    
    public B2WProjectBuilder() {
        super("b2w", B2WProjectBuilder.class, PROCEDURES, PARTITIONING);
    }

}
