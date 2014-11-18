package edu.mit.benchmark.affinity;

import java.io.IOException;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.ycsb.YCSBConstants;
import edu.brown.benchmark.ycsb.YCSBClient.Transaction;
import edu.brown.benchmark.ycsb.distributions.IntegerGenerator;
import edu.brown.benchmark.ycsb.distributions.UniformIntegerGenerator;
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
        READ_A("Read Record", AffinityConstants.FREQ_READ_A), 
        READ_B("Read Record", AffinityConstants.FREQ_READ_B); 
        
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


    private IntegerGenerator keyGenerator;
    private Random rand_gen;
    private int init_record_count;
    private final FlatHistogram<Transaction> txnWeights;

  

    public AffinityClient(String[] args) {
        
        super(args);

        this.rand_gen = new Random(); 
        init_record_count= AffinityConstants.NUM_RECORDS;
        this.keyGenerator = new UniformIntegerGenerator(rand_gen, 0, this.init_record_count);
        for (String key : m_extraParams.keySet()) {

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
        params = new Object[]{ this.keyGenerator.nextInt() };
        final Transaction target = this.txnWeights.nextValue();

        Callback callback = new Callback(target.ordinal());
        return this.getClientHandle().callProcedure(callback, target.callName, params);
    }

    @Override
    protected String[] getTransactionDisplayNames() {
        // TODO Auto-generated method stub
        return null;
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
    
}
