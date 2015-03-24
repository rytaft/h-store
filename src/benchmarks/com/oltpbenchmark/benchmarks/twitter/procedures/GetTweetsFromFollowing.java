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

import com.oltpbenchmark.benchmarks.twitter.TwitterConstants;

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
        " WHERE f1 = ? LIMIT " + TwitterConstants.LIMIT_FOLLOWERS
    );
    
    /** NOTE: The ?? is substituted into a string of repeated ?'s */
    public final SQLStmt getTweets = new SQLStmt(
        "SELECT * FROM " + TwitterConstants.TABLENAME_TWEETS +
        " WHERE uid = ? LIMIT " + TwitterConstants.LIMIT_TWEETS_FOR_UID
    );
    
    public VoltTable[] run(int uid) {
    	voltQueueSQL(getFollowing, uid);
    	VoltTable result[] = voltExecuteSQL();
        
        int num_params = Math.min(result[0].getRowCount(), TwitterConstants.LIMIT_FOLLOWERS);
        for(int i = 0; i < num_params; ++i) {
        	voltQueueSQL(getTweets, result[0].fetchRow(i).getLong(0));
        }
        
        if (num_params > 0) {
            return voltExecuteSQL(true);
        }
        
        return null;
    }
    
}
