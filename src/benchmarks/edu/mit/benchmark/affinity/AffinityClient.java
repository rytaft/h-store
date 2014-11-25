package edu.mit.benchmark.affinity;

import java.io.IOException;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;

public class AffinityClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(AffinityClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }

    public static enum Transaction {
        GET_SUPPLIER("Get Supplier", AffinityConstants.FREQ_READ_SUPPLIER), 
        GET_PRODUCT("Get Product", AffinityConstants.FREQ_READ_PRODUCT),
        GET_PART("Get Part", AffinityConstants.FREQ_READ_PART),
        GET_PARTS_BY_SUPPLIER("Get Parts By Supplier", AffinityConstants.FREQ_READ_PARTS_BY_SUPPLIER),
        GET_PARTS_BY_PRODUCT("Get Parts By Product", AffinityConstants.FREQ_READ_PARTS_BY_PRODUCT); 
        
        /**
         * Constructor
         */
        private Transaction(String displayName, int weight) {
            this.displayName = displayName;
            this.callName = displayName.replace(" ", "");
            this.weight = weight;
        }
        
        public final String displayName;
        public final String callName;
        public final int weight; // probability (in terms of percentage) the transaction gets executed
    
    } // TRANSCTION ENUM
    
    
    public static void main(String args[]) {
        BenchmarkComponent.main(AffinityClient.class, args, false);
    }


    private final int init_supplier_count;
    private final int init_product_count;
    private final int init_part_count;
    private Random rand_gen;
    private final FlatHistogram<Transaction> txnWeights;

  

    public AffinityClient(String[] args) {
        super(args);     
        
        this.rand_gen = new Random(); 
        boolean useFixedSize = true;
        int num_suppliers = AffinityConstants.NUM_SUPPLIERS;
        int num_products = AffinityConstants.NUM_PRODUCTS;
        int num_parts = AffinityConstants.NUM_PARTS;
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            if  (key.equalsIgnoreCase("fixed_size")) {
            	useFixedSize = Boolean.valueOf(value);
            }
            // Used Fixed-size Database
            // Parameter that points to where we can find the initial data files
            else if  (key.equalsIgnoreCase("num_suppliers")) {
                num_suppliers = Integer.valueOf(value);
            }
            else if  (key.equalsIgnoreCase("num_products")) {
            	num_products = Integer.valueOf(value);
            }
            else if  (key.equalsIgnoreCase("num_parts")) {
            	num_parts = Integer.valueOf(value);
            }
        } // FOR
        
        // Figure out the # of records that we need
        if (useFixedSize) {
            this.init_supplier_count = num_suppliers;
            this.init_product_count = num_products;
            this.init_part_count = num_parts;
        }
        else {
            this.init_supplier_count = (int) Math.round(AffinityConstants.NUM_SUPPLIERS * this.getScaleFactor());
            this.init_product_count = (int) Math.round(AffinityConstants.NUM_PRODUCTS * this.getScaleFactor());
            this.init_part_count = (int) Math.round(AffinityConstants.NUM_PARTS * this.getScaleFactor());
        }

        
        // Initialize the sampling table
        Histogram<Transaction> txns = new ObjectHistogram<Transaction>(); 
        for (Transaction t : Transaction.values()) {
            String propOverride = t.callName+"Proportion";
            propOverride = propOverride.toUpperCase();
            if(m_extraParams.containsKey(propOverride)){
                if(debug.val) LOG.debug("Using override for operation weight for " + propOverride);
                Float weightFlt = Float.valueOf(m_extraParams.get(propOverride));
                
                if(weightFlt<1){
                    weightFlt*=100;
                }
                txns.put(t,weightFlt.intValue());
            }
            else{
                Integer weight = this.getTransactionWeight(t.callName);
                if (weight == null) weight = t.weight;
                txns.put(t, weight);
            }
        } // FOR
        assert(txns.getSampleCount() == 100) : txns;
        this.txnWeights = new FlatHistogram<Transaction>(this.rand_gen, txns);
    }

    @SuppressWarnings("unused")
    @Deprecated
    @Override
    public void runLoop() {
        try {
            Client client = this.getClientHandle();
            Random rand = new Random();
            int key = -1; 
            int scan_count; 
            while (true) {
                runOnce();
            } 
        } 
        catch (IOException e) {
            
        }
    }
    
    
    @Override
    protected boolean runOnce() throws IOException {
        Object params[];

        final Transaction target = this.txnWeights.nextValue();
        switch (target) {
            case GET_SUPPLIER:
                params = new Object[]{ generateSupplier() };
                break;
            case GET_PRODUCT:
                params = new Object[]{ generateProduct() };
                break;
            case GET_PART:
                params = new Object[]{ generatePart() };
                break;
            case GET_PARTS_BY_SUPPLIER:
                params = new Object[]{ generateSupplier() };
                break;
            case GET_PARTS_BY_PRODUCT:
                params = new Object[]{ generateProduct() };
                break;
            default:
                throw new RuntimeException("Unexpected txn '" + target + "'");
        }
        assert(params != null);

        //LOG.info("calling : " + target +  " o:"+target.ordinal() + " : " + target.callName);
        Callback callback = new Callback(target.ordinal());
        return this.getClientHandle().callProcedure(callback, target.callName, params);
    }
    
    // @TODO add different distributions - hot spot, zipfian, etc
    private int generateSupplier() {
    	return this.rand_gen.nextInt(init_supplier_count);
    }
    
    private int generateProduct() {
    	return this.rand_gen.nextInt(init_product_count);
    }

    private int generatePart() {
    	return this.rand_gen.nextInt(init_part_count);
    }
    
    private class Callback implements ProcedureCallback {
        private final int idx;

        public Callback(int idx) {
            this.idx = idx;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, this.idx);

        }
    } // END CLASS
    
    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[AffinityProjectBuilder.PROCEDURES.length];
        for (int i = 0; i < procNames.length; i++) {
            procNames[i] = AffinityProjectBuilder.PROCEDURES[i].getSimpleName();
        }
        return (procNames);
    }
    
}
