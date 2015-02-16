/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.benchmarks.twitter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.benchmark.Clock;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.TimestampType;

import com.oltpbenchmark.api.TransactionGenerator;
import com.oltpbenchmark.benchmarks.twitter.util.TraceTransactionGenerator;
import com.oltpbenchmark.benchmarks.twitter.util.TransactionSelector;
import com.oltpbenchmark.benchmarks.twitter.util.TweetHistogram;
import com.oltpbenchmark.benchmarks.twitter.util.TwitterOperation;
import com.oltpbenchmark.util.TextGenerator;

import edu.brown.api.BenchmarkComponent;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.StringUtil;

public class TwitterClient extends BenchmarkComponent {
	private static final Logger LOG = Logger.getLogger(TwitterClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    private class TwitterCallback implements ProcedureCallback {
        private final int idx;
 
        public TwitterCallback(int idx) {
            super();
            this.idx = idx;
        }
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            //if (debug.val) LOG.debug(clientResponse);
            
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, this.idx);
        }
    } // END CLASS
    
    private final FlatHistogram<Transaction> txnWeights;
    private final Clock clock;

    // Callbacks
    protected final TwitterCallback callbacks[];


    public static enum Transaction {
    	GET_TWEET("Get Tweet", TwitterConstants.FREQ_GET_TWEET), 
    	GET_TWEETS_FROM_FOLLOWING("Get Tweets From Following", TwitterConstants.FREQ_GET_TWEETS_FROM_FOLLOWING),
    	GET_FOLLOWERS("Get Followers", TwitterConstants.FREQ_GET_FOLLOWERS),
    	GET_USER_TWEETS("Get User Tweets", TwitterConstants.FREQ_GET_USER_TWEETS),
    	INSERT_TWEET("Insert Tweet", TwitterConstants.FREQ_INSERT_TWEET); 
        
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
    
    private TransactionGenerator<TwitterOperation> generator;

    private final FlatHistogram<Integer> tweet_len_rng;
    private final int num_users;
    private String tweets_file;
    private String users_file;
    
    private Random rng = new Random();
    
    public TwitterClient(String[] args) { //int id, TwitterBenchmark benchmarkModule, TransactionGenerator<TwitterOperation> generator) {
        super(args);
        
        // Initialize the sampling table
        ObjectHistogram<Transaction> txns = new ObjectHistogram<Transaction>(); 
        for (Transaction t : Transaction.values()) {
            Integer weight = this.getTransactionWeight(t.callName);
            if (weight == null) weight = t.weight;
            txns.put(t, weight);
        } // FOR
        assert(txns.getSampleCount() == 100) : txns;
        this.txnWeights = new FlatHistogram<Transaction>(this.rng, txns);
        if (debug.val) LOG.debug("Transaction Workload Distribution:\n" + txns);
        
        // Setup callbacks
        int num_txns = Transaction.values().length;
        this.callbacks = new TwitterCallback[num_txns];
        for (int i = 0; i < num_txns; i++) {
            this.callbacks[i] = new TwitterCallback(i);
        } // FOR
        
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            if  (key.equalsIgnoreCase("tweets_file")) {
                tweets_file = String.valueOf(value);
            }
            else if  (key.equalsIgnoreCase("users_file")) {
                users_file = String.valueOf(value);
            }
        }
        
        try {
        	TransactionSelector transSel = new TransactionSelector(
        		tweets_file, 
        		users_file);
        	List<TwitterOperation> trace = Collections.unmodifiableList(transSel.readAll());
        	transSel.close();

        	this.generator = new TraceTransactionGenerator(trace);
        	this.num_users = (int)Math.round(TwitterConstants.NUM_USERS * this.getScaleFactor());
    	}
        catch(Exception ex) {
            throw new RuntimeException(ex);
        }
        
        TweetHistogram tweet_h = new TweetHistogram();
        this.tweet_len_rng = new FlatHistogram<Integer>(this.rng, tweet_h);
        this.clock = new Clock.RealTime();
    }
    
    public static void main(String args[]) {
        BenchmarkComponent.main(TwitterClient.class, args, false);
    }
    
    /**
     * Benchmark execution loop
     */
    @Override
    public void runLoop() {
        if (debug.val) LOG.debug("Starting runLoop()");
        Client client = this.getClientHandle();
        try {
            while (true) {
                // Figure out what page they're going to update
                this.runOnce();
                client.backpressureBarrier();
            } // WHILE
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    @Override
	protected boolean runOnce() throws IOException {
        Transaction target = this.selectTransaction();
        this.startComputeTime(target.displayName);
        Object params[] = null;
        try {
            params = this.generateParams(target);
        } catch (Throwable ex) {
            throw new RuntimeException("Unexpected error when generating params for " + target, ex);
        } finally {
            this.stopComputeTime(target.displayName);
        }
        assert(params != null);
        boolean ret = this.getClientHandle().callProcedure(this.callbacks[target.ordinal()],
                                                           target.callName,
                                                           params);

        //if (debug.val) LOG.debug("Executing txn:" + target.callName + ",with params:" + params);
        return ret;
	}
    
    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[Transaction.values().length];
        for (int i = 0; i < procNames.length; i++) {
            procNames[i] = Transaction.values()[i].displayName;
        }
        return (procNames);
    }
    
    private Transaction selectTransaction() {
        return this.txnWeights.nextValue();
    }

    protected Object[] generateParams(Transaction txn) {
    	TwitterOperation t = generator.nextTransaction();
        t.uid = this.rng.nextInt(this.num_users); // HACK
        
        Object params[] = null;
        switch (txn) {
        case GET_TWEET:
        	params = new Object[]{
        			t.tweetid
        	};
        	break;
        case GET_TWEETS_FROM_FOLLOWING:
        	params = new Object[]{
        			t.uid
            };
            break;
        case GET_FOLLOWERS:
        	params = new Object[]{
        			t.uid
            };
            break;
        case GET_USER_TWEETS:
        	params = new Object[]{
        			t.uid
            };
            break;
        case INSERT_TWEET:
        	int len = this.tweet_len_rng.nextValue().intValue();
            String text = TextGenerator.randomStr(this.rng, len);
            TimestampType time = clock.getDateTime();
        	params = new Object[]{
        			t.uid,
        			text,
        			time
            };
            break;
        default:
            assert(false):"Should not come to this point";
        }
        assert(params != null);

        if (debug.val) LOG.debug(txn + " Params:\n" + StringUtil.join("\n", params));
        return params;
	}


}
