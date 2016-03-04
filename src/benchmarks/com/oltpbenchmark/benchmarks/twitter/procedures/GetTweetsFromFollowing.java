/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:    Carlo Curino <carlo.curino@gmail.com>
 *              Evan Jones <ej@evanjones.ca>
 *              DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 *              Andy Pavlo <pavlo@cs.brown.edu>
 *              CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *                  Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.benchmarks.twitter.procedures;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.oltpbenchmark.benchmarks.twitter.TwitterConstants;

import edu.brown.benchmark.ycsb.distributions.ZipfianGenerator;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
		partitionInfo = "FOLLOWS.F1: 0",
        singlePartition = true
)
public class GetTweetsFromFollowing extends VoltProcedure {

    public final SQLStmt getFollowing = new SQLStmt(
        "SELECT f2 FROM " + TwitterConstants.TABLENAME_FOLLOWS +
        " WHERE f1 = ?"
    );
    
    /** NOTE: The ?? is substituted into a string of repeated ?'s */
    public final SQLStmt getTweets = new SQLStmt(
        "SELECT * FROM " + TwitterConstants.TABLENAME_TWEETS +
        " WHERE uid = ? LIMIT " + TwitterConstants.LIMIT_TWEETS_FOR_UID
    );
    
    public VoltTable[] run(int uid) {
    	voltQueueSQL(getFollowing, uid);
    	final VoltTable result[] = voltExecuteSQL();
    	assert result.length == 1;
        
    	if (Math.min(result[0].getRowCount(), TwitterConstants.LIMIT_FOLLOWING) > 0) {
            long[] following = new long[result[0].getRowCount()];

            // get the list of users that uid is following
            for (int i = 0; i < result[0].getRowCount(); ++i) {
                following[i] = result[0].fetchRow(i).getLong(0);
            }

            // The chosen set of users will follow a zipfian distribution
            // without replacement
            Arrays.sort(following);
            ZipfianGenerator r = new ZipfianGenerator(following.length);
            Set<Integer> indices = new HashSet<>();
            for (int i = 0; i < TwitterConstants.LIMIT_FOLLOWING && i < following.length; ++i) {
                Integer index = r.nextInt();
                while (indices.contains(index)) {
                    index = (index + 1) % following.length;
                }
                indices.add(index);
            }
            for (Integer index : indices) {
                voltQueueSQL(getTweets, following[index]);
            }
        
            return voltExecuteSQL(true);
        }

        
        return null;
    }
    
}
