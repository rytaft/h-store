/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original By: VoltDB Inc.											   *
 *  Ported By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)  *								   
 *                                                                         *
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

package edu.brown.benchmark.voter;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import weka.classifiers.meta.Vote;
import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.ycsb.distributions.Utils;
import edu.brown.benchmark.ycsb.distributions.IntegerGenerator;
import edu.brown.benchmark.ycsb.distributions.UniformIntegerGenerator;
import edu.brown.benchmark.ycsb.distributions.VaryingZipfianGenerator;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class VoterClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(VoterClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    
    // parameters for zipfian skew
    private double skewFactor = VaryingZipfianGenerator.ZIPFIAN_CONSTANT;
    private boolean scrambled = false;
    private boolean mirrored = false;
    private long interval = VaryingZipfianGenerator.DEFAULT_INTERVAL;
    private long shift = VaryingZipfianGenerator.DEFAULT_SHIFT;
    private boolean zipfian = false;
    private int numHotSpots = 0;
    private double percentAccessHotSpots = 0.0;
    private boolean randomShift = false;
    private final IntegerGenerator areaCodeGenerator;
    private final IntegerGenerator phoneNumberGenerator;

    // Phone number generator
    PhoneCallGenerator switchboard;

    // Flags to tell the worker threads to stop or go
    AtomicBoolean warmupComplete = new AtomicBoolean(false);
    AtomicBoolean benchmarkComplete = new AtomicBoolean(false);

    // voter benchmark state
    AtomicLong acceptedVotes = new AtomicLong(0);
    AtomicLong badContestantVotes = new AtomicLong(0);
    AtomicLong badVoteCountVotes = new AtomicLong(0);
    AtomicLong failedVotes = new AtomicLong(0);

    final Callback callback = new Callback();

    public static void main(String args[]) {
        BenchmarkComponent.main(VoterClient.class, args, false);
    }

    public VoterClient(String args[]) {
        super(args);
        int numContestants = VoterUtil.getScaledNumContestants(this.getScaleFactor());
        this.switchboard = new PhoneCallGenerator(this.getClientId(), numContestants);
        
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            // Zipfian Skew Factor
            if (key.equalsIgnoreCase("skew_factor")) {
                this.skewFactor = Double.valueOf(value);
                this.zipfian = true;
            }
            // Whether or not to scramble the zipfian distribution
            else if (key.equalsIgnoreCase("scrambled")) {
                this.scrambled = Boolean.valueOf(value);
            }
            // Whether or not to mirror the zipfian distribution
            else if (key.equalsIgnoreCase("mirrored")) {
                this.mirrored = Boolean.valueOf(value);
            }
            // Interval for changing skew distribution
            else if (key.equalsIgnoreCase("interval")) {
                this.interval = Long.valueOf(value);
            }
            // Whether to use a random shift
            else if (key.equalsIgnoreCase("random_shift")) {
                this.randomShift = Boolean.valueOf(value);
            }
            // How much to shift the distribution each time
            else if (key.equalsIgnoreCase("shift")) {
                this.shift = Long.valueOf(value);
            }
            // Number of hot spots
            else if (key.equalsIgnoreCase("num_hot_spots")) {
                this.numHotSpots = Integer.valueOf(value);
            }
            // Percent of access going to the hot spots
            else if (key.equalsIgnoreCase("percent_accesses_to_hot_spots")) {
                this.percentAccessHotSpots = Double.valueOf(value);
            }
            else{
                if(debug.val) LOG.debug("Unknown prop : "  + key);
            }
        } // FOR
        
        if(zipfian) {
            if(debug.val) LOG.debug("Using a default zipfian key distribution");
            //ints are used for keyGens and longs are used for record counts.            
            //TODO check on other zipf params
            VaryingZipfianGenerator areaCodeGen = new VaryingZipfianGenerator(PhoneCallGenerator.AREA_CODES.length, skewFactor);
            areaCodeGen.setInterval(interval);
            areaCodeGen.setMirrored(mirrored);
            areaCodeGen.setNumHotSpots(numHotSpots);
            areaCodeGen.setPercentAccessHotSpots(percentAccessHotSpots);
            areaCodeGen.setRandomShift(randomShift);
            areaCodeGen.setScrambled(scrambled);
            areaCodeGen.setShift(shift);
            this.areaCodeGenerator = areaCodeGen;
            
            VaryingZipfianGenerator phoneNumberGen = new VaryingZipfianGenerator(10000000, skewFactor);
            phoneNumberGen.setInterval(interval);
            phoneNumberGen.setMirrored(mirrored);
            phoneNumberGen.setNumHotSpots(numHotSpots);
            phoneNumberGen.setPercentAccessHotSpots(percentAccessHotSpots);
            phoneNumberGen.setRandomShift(randomShift);
            phoneNumberGen.setScrambled(scrambled);
            phoneNumberGen.setShift(shift);
            this.phoneNumberGenerator = phoneNumberGen;
        }
        else {
            if(debug.val) LOG.debug("Using a uniform key distribution");
            //Ints are used for keyGens and longs are used for record counts.
            this.areaCodeGenerator = new UniformIntegerGenerator(Utils.random(), 0, PhoneCallGenerator.AREA_CODES.length);
            this.phoneNumberGenerator = new UniformIntegerGenerator(Utils.random(), 0, 10000000);
        }
        
    }

    @Override
    public void runLoop() {
        try {
            while (true) {
                // synchronously call the "Vote" procedure
                try {
                    runOnce();
                } catch (Exception e) {
                    failedVotes.incrementAndGet();
                }

            } // WHILE
        } catch (Exception e) {
            // Client has no clean mechanism for terminating with the DB.
            e.printStackTrace();
        }
    }

    @Override
    protected boolean runOnce() throws IOException {
        // Get the next phone call
        //PhoneCallGenerator.PhoneCall call = switchboard.receive();
    	PhoneCallGenerator.PhoneCall call = switchboard.receive(areaCodeGenerator, phoneNumberGenerator);

        Client client = this.getClientHandle();
        boolean response = client.callProcedure(callback,
                                                "Vote",
                                                call.voteId,
                                                call.phoneNumber,
                                                call.contestantNumber,
                                                VoterConstants.MAX_VOTES);
        return response;
    }

    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[]{
            Vote.class.getSimpleName()
        };
        return (procNames);
    }

    private class Callback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, 0);
            
            // Keep track of state (optional)
            if (clientResponse.getStatus() == Status.OK) {
                VoltTable results[] = clientResponse.getResults();
                assert(results.length == 1);
                long status = results[0].asScalarLong();
                if (status == VoterConstants.VOTE_SUCCESSFUL) {
                    acceptedVotes.incrementAndGet();
                }
                else if (status == VoterConstants.ERR_INVALID_CONTESTANT) {
                    badContestantVotes.incrementAndGet();
                }
                else if (status == VoterConstants.ERR_VOTER_OVER_VOTE_LIMIT) {
                    badVoteCountVotes.incrementAndGet();
                }
            }
            else if (clientResponse.getStatus() == Status.ABORT_UNEXPECTED) {
                if (clientResponse.getException() != null) {
                    clientResponse.getException().printStackTrace();
                }
                if (debug.val && clientResponse.getStatusString() != null) {
                    LOG.warn(clientResponse.getStatusString());
                }
            }
        }
    } // END CLASS
}
