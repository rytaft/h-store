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

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class GetTweetsFromFollowing extends VoltProcedure {

    public final SQLStmt getFollowing = new SQLStmt(
        "SELECT f2 FROM " + TwitterConstants.TABLENAME_FOLLOWS +
        " WHERE f1 = ? LIMIT " + TwitterConstants.LIMIT_FOLLOWERS
    );
    
    /** NOTE: The ?? is substituted into a string of repeated ?'s */
    public final SQLStmt getTweets = new SQLStmt(
        "SELECT * FROM " + TwitterConstants.TABLENAME_TWEETS +
        " WHERE uid IN (??)", TwitterConstants.LIMIT_FOLLOWERS
    );
    
    public VoltTable[] run(int uid) {
    	voltQueueSQL(getFollowing, uid);
    	VoltTable result[] = voltExecuteSQL();
        
        int num_params = Math.min(result[1].getRowCount(), TwitterConstants.LIMIT_FOLLOWERS);
        Object params[] = new Object[num_params];
        for(int i = 0; i < num_params; ++i) {
        	params[i] = result[1].fetchRow(i).getLong(0);
        }
        
        if (num_params > 0) {
            for(int i = num_params; i < TwitterConstants.LIMIT_FOLLOWERS; ++i) {
                params[i] = params[num_params-1];
            } // WHILE
            voltQueueSQL(getTweets, params);
            return voltExecuteSQL(true);
        }
        
        return null;
    }
    
}
