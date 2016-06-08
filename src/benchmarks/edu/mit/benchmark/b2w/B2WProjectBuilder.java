package edu.mit.benchmark.b2w;

import org.voltdb.VoltProcedure;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.AbstractProjectBuilder;

public class B2WProjectBuilder extends AbstractProjectBuilder{

    
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = B2WClient.class;
 
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = B2WLoader.class;
 
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[]) new Class<?>[] {

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
