package edu.mit.benchmark.b2w;

import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ThreadUtil;

public class B2WConfig {
    private static final Logger LOG = Logger.getLogger(B2WConfig.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
     
    public Random rand_gen;
    public Integer loadthreads = ThreadUtil.availableProcessors();;
    public boolean useFixedSize = true;

    
    public B2WConfig(Map<String, String> m_extraParams) {
        this.rand_gen = new Random(); 
        
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            // Used Fixed-size Database
            if  (key.equalsIgnoreCase("fixed_size")) {
                useFixedSize = Boolean.valueOf(value);
            }
            // Multi-Threaded Loader
            else if (key.equalsIgnoreCase("loadthreads")) {
                this.loadthreads  = Integer.valueOf(value);
            }
        } // FOR
            
    }

    
    
    @Override
    public String toString() {
        return "B2WConfig [loadthreads=" + loadthreads + ", useFixedSize=" + useFixedSize + "]";
    }

}
