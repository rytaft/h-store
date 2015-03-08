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
package com.oltpbenchmark.benchmarks.twitter.util;

import java.util.Random;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import com.oltpbenchmark.benchmarks.twitter.TwitterClient.Transaction;

import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.statistics.ObjectHistogram;

public class GraphTransactionGenerator {
	private final FlatHistogram<Long> followersWeights;
	private final FlatHistogram<Long> followingWeights;
	private Random rng = new Random();
	private final long num_users;
    private final long num_tweets;
    private Client client;

	
	/**
	 * @param transactions
	 *            a list of transactions shared between threads.
	 */
	public GraphTransactionGenerator(long num_users, long num_tweets, Client client) {
		this.num_users = num_users;
		this.num_tweets = num_tweets;
		this.client = client;
		
		try {
			///////////////
			// Followers //
			///////////////
			VoltTable[] result = this.client.callProcedure("GetFollowersCount").getResults();

			ObjectHistogram<Long> followersCounts = new ObjectHistogram<Long>(); 
			for(int i = 0; i < result[0].getRowCount(); ++i) {
				long weight = (long) Math.ceil(Math.log(result[0].fetchRow(i).getLong(1))) + 1;
				followersCounts.put(result[0].fetchRow(i).getLong(0), weight);
			}

			this.followersWeights = new FlatHistogram<Long>(this.rng, followersCounts);

			///////////////
			// Following //
			///////////////
			result = this.client.callProcedure("GetFollowingCount").getResults();
			
			ObjectHistogram<Long> followingCounts = new ObjectHistogram<Long>(); 
			for(int i = 0; i < result[0].getRowCount(); ++i) {
				long weight = (long) Math.ceil(Math.log(result[0].fetchRow(i).getLong(1))) + 1;
				followingCounts.put(result[0].fetchRow(i).getLong(0), weight);
			}

			this.followingWeights = new FlatHistogram<Long>(this.rng, followingCounts);
		}
		catch(Exception e) {
			throw new RuntimeException(e);
		}
	}

	public TwitterOperation nextTransaction(Transaction txn) {
		long uid = 0;
		long tweetid = 0;
		
        switch (txn) {
        case GET_TWEET:
        	tweetid = this.rng.nextInt() % this.num_tweets;
        	break;
        case GET_TWEETS_FROM_FOLLOWING:
        	uid = followingWeights.nextValue();
            break;
        case GET_FOLLOWERS:
        case GET_USER_TWEETS:
        case INSERT_TWEET:
        	uid = followersWeights.nextValue();
            break;
        default:
            assert(false):"Should not come to this point";
        }
	    return new TwitterOperation((int) tweetid, (int) uid);
	}
}
