package edu.mit.benchmark.affinity;

import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import edu.brown.benchmark.ycsb.distributions.CustomSkewGenerator;
import edu.brown.benchmark.ycsb.distributions.IntegerGenerator;
import edu.brown.benchmark.ycsb.distributions.UniformIntegerGenerator;
import edu.brown.benchmark.ycsb.distributions.VaryingZipfianGenerator;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ThreadUtil;

public class AffinityConfig {
    private static final Logger LOG = Logger.getLogger(AffinityConfig.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    public double SUPPLIES_PROBABILITY = AffinityConstants.SUPPLIES_PROBABILITY;

    public double USES_PROBABILITY = AffinityConstants.USES_PROBABILITY;

    public int FREQ_READ_SUPPLIER = AffinityConstants.FREQ_READ_SUPPLIER;
    public int FREQ_READ_PRODUCT = AffinityConstants.FREQ_READ_PRODUCT;
    public int FREQ_READ_PART = AffinityConstants.FREQ_READ_PART;
    public int FREQ_READ_PARTS_BY_SUPPLIER = AffinityConstants.FREQ_READ_PARTS_BY_SUPPLIER;
    public int FREQ_READ_PARTS_BY_PRODUCT = AffinityConstants.FREQ_READ_PARTS_BY_PRODUCT;

    public long num_suppliers = AffinityConstants.NUM_SUPPLIERS;
    public long num_products = AffinityConstants.NUM_PRODUCTS;
    public long num_parts = AffinityConstants.NUM_PARTS;
    public Random rand_gen;
    public IntegerGenerator supplier_gen;
    public IntegerGenerator product_gen;
    public IntegerGenerator part_gen;
    public Integer loadthreads = ThreadUtil.availableProcessors();;
    public boolean useFixedSize = true;

    
    public AffinityConfig(Map<String, String> m_extraParams) {
        this.rand_gen = new Random(); 

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
            }            // Used Fixed-size Database
            // Parameter that points to where we can find the initial data files
            else if (key.equalsIgnoreCase("fixed_size")) {
                useFixedSize = Boolean.valueOf(value);
            }
            // Multi-Threaded Loader
            else if (key.equalsIgnoreCase("loadthreads")) {
                this.loadthreads  = Integer.valueOf(value);
            }
            else{
                if (key.startsWith(AffinityConstants.PARTS_PRE) || key.startsWith(AffinityConstants.SUP_PRE) || key.startsWith(AffinityConstants.PROD_PRE))
                    continue;
                if(debug.val) LOG.debug("Unknown prop : "  + key);
            }
        } // FOR
        
        


   
    
        supplier_gen = getGenerator(AffinityConstants.SUP_PRE, num_suppliers, m_extraParams);
        product_gen = getGenerator(AffinityConstants.PROD_PRE, num_products, m_extraParams);
        part_gen = getGenerator(AffinityConstants.PARTS_PRE, num_parts, m_extraParams);
    }

    
    
    
    private IntegerGenerator getGenerator(String pre, long num_keys, Map<String, String> m_extraParams) {

        String requestDistribution = AffinityConstants.REQUEST_DISTRIBUTION_PROPERTY_DEFAULT; 

        double skewFactor = AffinityConstants.ZIPFIAN_CONSTANT;
        boolean scrambled = false;
        boolean mirrored = false;
        long interval = VaryingZipfianGenerator.DEFAULT_INTERVAL;
        long shift = VaryingZipfianGenerator.DEFAULT_SHIFT;
        int numHotSpots = 0;
        double percentAccessHotSpots = 0.0;
        boolean randomShift = false;
        boolean randomHotSpots = false;    

        IntegerGenerator keyGenerator;

        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            //single key distribution
            if (key.equalsIgnoreCase(pre+AffinityConstants.REQUEST_DISTRIBUTION_PROPERTY)){
                requestDistribution = value;
            }
            // Zipfian Skew Factor
            else if (key.equalsIgnoreCase(pre+"skew_factor")) {
                skewFactor = Double.valueOf(value);
            }
            // Whether or not to scramble the zipfian distribution
            else if (key.equalsIgnoreCase(pre+"scrambled")) {
                scrambled = Boolean.valueOf(value);
            }
            // Whether or not to mirror the zipfian distribution
            else if (key.equalsIgnoreCase(pre+"mirrored")) {
                mirrored = Boolean.valueOf(value);
            }
            // Interval for changing skew distribution
            else if (key.equalsIgnoreCase(pre+"interval")) {
                interval = Long.valueOf(value);
            }
            // Whether to use a random shift
            else if (key.equalsIgnoreCase(pre+"random_shift")) {
                randomShift = Boolean.valueOf(value);
            }
            // How much to shift the distribution each time (if not random)
            else if (key.equalsIgnoreCase(pre+"shift")) {
                shift = Long.valueOf(value);
            }
            // Number of hot spots
            else if (key.equalsIgnoreCase(pre+"num_hot_spots")) {
                numHotSpots = Integer.valueOf(value);
            }
            // Percent of access going to the hot spots
            else if (key.equalsIgnoreCase(pre+"percent_accesses_to_hot_spots")) {
                percentAccessHotSpots = Double.valueOf(value);
            }
        // Whether to make the location of the hot spots random
            else if (key.equalsIgnoreCase(pre+"random_hot_spots")) {
                randomHotSpots = Boolean.valueOf(value);
            }
        } // FOR
        // initialize distribution generators 
        // We must know where to start inserting
        if(requestDistribution.equals(AffinityConstants.CUSTOM_DISTRIBUTION)){
            if(debug.val) LOG.debug(pre+" Using a custom key distribution");
    
            keyGenerator = new CustomSkewGenerator(this.rand_gen, num_keys, 
                                                AffinityConstants.HOT_DATA_WORKLOAD_SKEW, AffinityConstants.HOT_DATA_SIZE, 
                                                AffinityConstants.WARM_DATA_WORKLOAD_SKEW, AffinityConstants.WARM_DATA_SIZE);

        } 
        else if(requestDistribution.equals(AffinityConstants.UNIFORM_DISTRIBUTION)){
            if(debug.val) LOG.debug(pre+" Using a uniform key distribution");
            //Ints are used for keyGens and longs are used for record counts.
            keyGenerator = new UniformIntegerGenerator(this.rand_gen,0,(int)num_keys);
        }
        else if(requestDistribution.equals(AffinityConstants.ZIPFIAN_DISTRIBUTION)){
            if(debug.val) LOG.debug(pre+" Using a default zipfian key distribution");
            //ints are used for keyGens and longs are used for record counts.            
            //TODO check on other zipf params
            VaryingZipfianGenerator gen = new VaryingZipfianGenerator(num_keys, skewFactor);
            gen.setInterval(interval);
            gen.setMirrored(mirrored);
            gen.setRandomHotSpots(randomHotSpots);
            gen.setNumHotSpots(numHotSpots);
            gen.setPercentAccessHotSpots(percentAccessHotSpots);
            gen.setRandomShift(randomShift);
            gen.setScrambled(scrambled);
            gen.setShift(shift);
            keyGenerator = gen;
        }
        else{
            String msg = "Unsupported affinity key " + pre +" distribution type :" + requestDistribution;
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
        
        
        return keyGenerator;
    }




    @Override
    public String toString() {
        return "AffinityConfig [SUPPLIES_PROBABILITY=" + SUPPLIES_PROBABILITY + ", USES_PROBABILITY=" + USES_PROBABILITY + ", FREQ_READ_SUPPLIER=" + FREQ_READ_SUPPLIER + ", FREQ_READ_PRODUCT="
                + FREQ_READ_PRODUCT + ", FREQ_READ_PART=" + FREQ_READ_PART + ", FREQ_READ_PARTS_BY_SUPPLIER=" + FREQ_READ_PARTS_BY_SUPPLIER + ", FREQ_READ_PARTS_BY_PRODUCT="
                + FREQ_READ_PARTS_BY_PRODUCT + ", num_suppliers=" + num_suppliers + ", num_products=" + num_products + ", num_parts=" + num_parts + ", rand_gen=" + rand_gen + ", supplier_gen="
                + supplier_gen + ", product_gen=" + product_gen + ", part_gen=" + part_gen + ", loadthreads=" + loadthreads + ", useFixedSize=" + useFixedSize + "]";
    }

}
