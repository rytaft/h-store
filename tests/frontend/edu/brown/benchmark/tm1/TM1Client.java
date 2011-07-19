/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original Version:                                                      *
 *  Zhe Zhang (zhe@cs.brown.edu)                                           *
 *                                                                         *
 *  Modifications by:                                                      *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.benchmark.tm1;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.benchmark.BenchmarkComponent;

/**
 * TM1Client
 * 
 * @author zhe
 * @author pavlo
 */
public class TM1Client extends TM1BaseClient {
    private static final Logger LOG = Logger.getLogger(TM1Client.class);

    /**
     * Each Transaction element provides an ArgGenerator to create the proper
     * arguments used to invoke the stored procedure
     *
     */
    private static interface ArgGenerator {
        /**
         * Generate the proper arguments used to invoke the given stored procedure
         * @param subscriberSize
         * @return
         */
        public Object[] genArgs(long subscriberSize);
    }

    /**
     * Set of transactions structs with their appropriate parameters
     */
    public static enum Transaction {
        DELETE_CALL_FORWARDING("Delete Call Forwarding", TM1Constants.FREQUENCY_DELETE_CALL_FORWARDING, new ArgGenerator() {
            public Object[] genArgs(long subscriberSize) {
                long s_id = TM1Util.getSubscriberId(subscriberSize);
                return new Object[] {
                        TM1Util.padWithZero(s_id), // s_id
                        TM1Util.number(1, 4), // sf_type
                        8 * TM1Util.number(0, 2) // start_time
                };
            }
        }),
        GET_ACCESS_DATA("Get Access Data", TM1Constants.FREQUENCY_GET_ACCESS_DATA, new ArgGenerator() {
            public Object[] genArgs(long subscriberSize) {
                long s_id = TM1Util.getSubscriberId(subscriberSize);
                return new Object[] {
                        s_id, // s_id
                        TM1Util.number(1, 4) // ai_type
                };
            }
        }),
        GET_NEW_DESTINATION("Get New Destination", TM1Constants.FREQUENCY_GET_NEW_DESTINATION, new ArgGenerator() {
            public Object[] genArgs(long subscriberSize) {
                long s_id = TM1Util.getSubscriberId(subscriberSize);
                return new Object[] {
                        s_id, // s_id
                        TM1Util.number(1, 4), // sf_type
                        8 * TM1Util.number(0, 2), // start_time
                        TM1Util.number(1, 24) // end_time
                };
            }
        }),
        GET_SUBSCRIBER_DATA("Get Subscriber Data", TM1Constants.FREQUENCY_GET_SUBSCRIBER_DATA, new ArgGenerator() {
            public Object[] genArgs(long subscriberSize) {
                long s_id = TM1Util.getSubscriberId(subscriberSize);
                return new Object[] {
                        s_id // s_id
                };
            }
        }),
        INSERT_CALL_FORWARDING("Insert Call Forwarding", TM1Constants.FREQUENCY_INSERT_CALL_FORWARDING, new ArgGenerator() {
            public Object[] genArgs(long subscriberSize) {
                long s_id = TM1Util.getSubscriberId(subscriberSize);
                return new Object[] {
                        TM1Util.padWithZero(s_id), // sub_nbr
                        TM1Util.number(1, 4), // sf_type
                        8 * TM1Util.number(0, 2), // start_time
                        TM1Util.number(1, 24), // end_time
                        TM1Util.padWithZero(s_id) // numberx
                };
            }
        }),
        UPDATE_LOCATION("Update Location", TM1Constants.FREQUENCY_UPDATE_LOCATION, new ArgGenerator() {
            public Object[] genArgs(long subscriberSize) {
                long s_id = TM1Util.getSubscriberId(subscriberSize);
                return new Object[] {
                        TM1Util.number(0, Integer.MAX_VALUE), // vlr_location
                        TM1Util.padWithZero(s_id) // sub_nbr
                };
            }
        }),
        UPDATE_SUBSCRIBER_DATA("Update Subscriber Data", TM1Constants.FREQUENCY_UPDATE_SUBSCRIBER_DATA, new ArgGenerator() {
            public Object[] genArgs(long subscriberSize) {
                long s_id = TM1Util.getSubscriberId(subscriberSize);
                return new Object[] {
                        s_id, // s_id
                        TM1Util.number(0, 1), // bit_1
                        TM1Util.number(0, 255), // data_a
                        TM1Util.number(1, 4) // sf_type
                };
            }
        }),
        ; // END LIST OF STORED PROCEDURES

        /**
         * Constructor
         */
        private Transaction(String displayName, int weight, ArgGenerator ag) {
            this.displayName = displayName;
            this.callName = displayName.replace(" ", "");
            this.weight = weight;
            this.ag = ag;
        }

        public final String displayName;
        public final String callName;
        public final int weight; // probability (in terms of percentage) the transaction gets executed
        public final ArgGenerator ag;
    } // TRANSCTION ENUM

    /**
     * Callback Class
     */
    protected class TM1Callback implements ProcedureCallback {
        private final int txn_id;
        
        public TM1Callback(int txn_id) {
            super();
            this.txn_id = txn_id;
        }
        
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(this.txn_id);
        }
    } // END CLASS

    /**
     * Data Members
     */
    
    // Storing the ordinals of transaction per tm1 probability distribution
    private final int[] SAMPLE_TABLE = new int[100];

    // Callbacks
    protected final TM1Callback callbacks[];

    /**
     * Main method
     * @param args
     */
    public static void main(String[] args) {
        BenchmarkComponent.main(TM1Client.class, args, false);
    }

    /**
     * Constructor
     * @param args
     */
    public TM1Client(String args[]) {
        super(args);
        this.initSampleTable();
        
        // Setup callbacks
        int num_txns = Transaction.values().length;
        this.callbacks = new TM1Callback[num_txns];
        for (int i = 0; i < num_txns; i++) {
            this.callbacks[i] = new TM1Callback(i);
        } // FOR
    }
    
    /**
     * Initialize the sampling table
     */
    private void initSampleTable() {
        int i = 0;
        int sum = 0;
        for (Transaction t : Transaction.values()) {
            for (int ii = 0; ii < t.weight; ii++) {
                SAMPLE_TABLE[i++] = t.ordinal();
            }
            sum += t.weight;
        }
        assert (100 == sum);
    }
    
    /**
     * Return a transaction randomly selected per TM1 probability specs
     */
    private Transaction selectTransaction() {
        Transaction force = null; // (this.getClientId() == 0 ? Transaction.INSERT_CALL_FORWARDING : Transaction.GET_SUBSCRIBER_DATA); // Transaction.INSERT_CALL_FORWARDING;
        if (force != null) return (force);
        return Transaction.values()[SAMPLE_TABLE[TM1Util.number(0,99).intValue()]];
    }

    /**
     * Benchmark execution loop
     */
    @Override
    public void runLoop() {
        LOG.debug("Starting runLoop()");
        try {
            while (true) {
                this.runOnce();
                this.getClientHandle().backpressureBarrier();
            } // WHILE
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    @Override
    protected boolean runOnce() throws IOException {
        final Transaction target = this.selectTransaction();
        boolean ret = this.getClientHandle().callProcedure(
                                   this.callbacks[target.ordinal()],
                                   target.callName,
                                   target.ag.genArgs(subscriberSize));
        LOG.debug("Executing txn " + target);
        return (ret);
    }

    @Override
    public String[] getTransactionDisplayNames() {
        // wish Java has MAP like in Lisp...
        String names[] = new String[Transaction.values().length];
        int ii = 0;
        for (Transaction transaction : Transaction.values()) {
            names[ii++] = transaction.displayName;
        }
        return names;
    }
}