package edu.mit.benchmark.affinity;

import org.voltdb.VoltProcedure;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.mit.benchmark.affinity.procedures.GetPart;
import edu.mit.benchmark.affinity.procedures.GetSupplier;
import edu.mit.benchmark.affinity.procedures.GetProduct;
import edu.mit.benchmark.affinity.procedures.GetPartsBySupplier;
import edu.mit.benchmark.affinity.procedures.GetPartsByProduct;

public class AffinityProjectBuilder extends AbstractProjectBuilder{

    
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = AffinityClient.class;
 
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = AffinityLoader.class;
 
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[]) new Class<?>[] {
        GetSupplier.class,
        GetProduct.class,
        GetPart.class,
        GetPartsBySupplier.class,
        GetPartsByProduct.class,
    };
    public static final String PARTITIONING[][] = new String[][] {
        // { "TABLE NAME", "PARTITIONING COLUMN NAME" }
        {"SUPPLIERS", "SUPPLIER_KEY"},
        {"PRODUCTS", "PRODUCT_KEY"},
        //{"PARTS", "PART_KEY"},
        {"SUPPLIES", "SUPPLIER_KEY"},
        {"USES", "PRODUCT_KEY"},
    };
    
    public AffinityProjectBuilder() {
        super("affinity", AffinityProjectBuilder.class, PROCEDURES, PARTITIONING);
    }

}
