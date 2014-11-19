package edu.mit.benchmark.affinity;

import org.voltdb.VoltProcedure;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.mit.benchmark.affinity.procedures.GetA;
import edu.mit.benchmark.affinity.procedures.GetB;
import edu.mit.benchmark.affinity.procedures.GetCByA;
import edu.mit.benchmark.affinity.procedures.GetCByB;

public class AffinityProjectBuilder extends AbstractProjectBuilder{

    
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = AffinityClient.class;
 
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = AffinityLoader.class;
 
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[]) new Class<?>[] {
        GetA.class,
        GetB.class,
        GetCByA.class,
        GetCByB.class,
    };
    public static final String PARTITIONING[][] = new String[][] {
        // { "TABLE NAME", "PARTITIONING COLUMN NAME" }
        {"TABLEA", "A_KEY"},
        {"TABLEB", "B_KEY"},
        {"TABLEACMAP", "A_KEY"},
        {"TABLEBCMAP", "B_KEY"},
    };
    
    public AffinityProjectBuilder() {
        super("affinity", AffinityProjectBuilder.class, PROCEDURES, PARTITIONING);
    }

}
