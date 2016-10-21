package edu.mit.benchmark.b2w;

import org.voltdb.VoltProcedure;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.mit.benchmark.b2w.procedures.AddLineToCheckout;
import edu.mit.benchmark.b2w.procedures.CancelStockReservation;
import edu.mit.benchmark.b2w.procedures.DeleteCart;
import edu.mit.benchmark.b2w.procedures.DeleteCheckout;
import edu.mit.benchmark.b2w.procedures.DeleteLineFromCheckout;
import edu.mit.benchmark.b2w.procedures.GetStockTransaction;
import edu.mit.benchmark.b2w.procedures.ReserveCart;
import edu.mit.benchmark.b2w.procedures.AddLineToCart;
import edu.mit.benchmark.b2w.procedures.CreateCheckout;
import edu.mit.benchmark.b2w.procedures.CreateCheckoutPayment;
import edu.mit.benchmark.b2w.procedures.CreateStockTransaction;
import edu.mit.benchmark.b2w.procedures.DeleteLineFromCart;
import edu.mit.benchmark.b2w.procedures.GetCart;
import edu.mit.benchmark.b2w.procedures.GetCheckout;
import edu.mit.benchmark.b2w.procedures.GetStock;
import edu.mit.benchmark.b2w.procedures.GetStockQuantity;
import edu.mit.benchmark.b2w.procedures.PurchaseStock;
import edu.mit.benchmark.b2w.procedures.ReserveStock;
import edu.mit.benchmark.b2w.procedures.UpdateStockTransaction;

public class B2WProjectBuilder extends AbstractProjectBuilder{

    
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = B2WClient.class;
 
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = B2WLoader.class;
 
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[]) new Class<?>[] {
        AddLineToCart.class,
        AddLineToCheckout.class,
        CancelStockReservation.class,
        CreateCheckout.class,
        CreateCheckoutPayment.class,
        CreateStockTransaction.class,
        DeleteCart.class,
        DeleteCheckout.class,
        DeleteLineFromCart.class,
        DeleteLineFromCheckout.class,
        GetCart.class,
        GetCheckout.class,
        GetStock.class,
        GetStockQuantity.class,
        GetStockTransaction.class,
        PurchaseStock.class,
        ReserveCart.class,
        ReserveStock.class,
        UpdateStockTransaction.class
    };
    
    public static final String PARTITIONING[][] = new String[][] {
        // { "TABLE NAME", "PARTITIONING COLUMN NAME" }
        {"STK_INVENTORY_STOCK", "partition_key"},
        {"STK_INVENTORY_STOCK_QUANTITY", "partition_key"},
        {"STK_STOCK_TRANSACTION", "partition_key"},
        {"CART", "partition_key"},
        {"CART_CUSTOMER", "partition_key"},
        {"CART_LINES", "partition_key"},
        {"CART_LINE_PRODUCTS", "partition_key"},
        {"CART_LINE_PROMOTIONS", "partition_key"},
        {"CART_LINE_PRODUCT_WARRANTIES", "partition_key"},
        {"CART_LINE_PRODUCT_STORES", "partition_key"},
        {"CHECKOUT", "partition_key"},
        {"CHECKOUT_PAYMENTS", "partition_key"},
        {"CHECKOUT_FREIGHT_DELIVERY_TIME", "partition_key"},
        {"CHECKOUT_STOCK_TRANSACTIONS", "partition_key"},
    };
    
    public B2WProjectBuilder() {
        super("b2w", B2WProjectBuilder.class, PROCEDURES, PARTITIONING);
    }

}
