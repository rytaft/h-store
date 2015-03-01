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

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;
import com.oltpbenchmark.benchmarks.twitter.procedures.GetFollowers;
import com.oltpbenchmark.benchmarks.twitter.procedures.GetTweet;
import com.oltpbenchmark.benchmarks.twitter.procedures.GetTweetsFromFollowing;
import com.oltpbenchmark.benchmarks.twitter.procedures.GetUserTweets;
import com.oltpbenchmark.benchmarks.twitter.procedures.InsertTweet;


public class TwitterProjectBuilder extends AbstractProjectBuilder {
    
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = TwitterClient.class;
 
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = TwitterLoader.class;
 
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        GetFollowers.class,
        GetTweet.class,
        GetTweetsFromFollowing.class,
        GetUserTweets.class,
        InsertTweet.class,
    };
    
    public static final String PARTITIONING[][] = new String[][] {
        { TwitterConstants.TABLENAME_USER, "uid" },
        { TwitterConstants.TABLENAME_TWEETS, "uid" },
        { TwitterConstants.TABLENAME_FOLLOWS, "f1" },
        { TwitterConstants.TABLENAME_FOLLOWERS, "f1" },
        { TwitterConstants.TABLENAME_ADDED_TWEETS, "uid" }
    };
 
    public TwitterProjectBuilder() {
        super("twitter", TwitterProjectBuilder.class, PROCEDURES, PARTITIONING);
    }

}
