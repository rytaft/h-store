package com.oltpbenchmark.benchmarks.twitter.procedures;

import com.oltpbenchmark.benchmarks.twitter.TwitterConstants;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

public class InsertTweet extends VoltProcedure {

	//FIXME: Carlo is this correct? 1) added_tweets is empty initially 2) id is supposed to be not null
    public final SQLStmt insertTweet = new SQLStmt(
        "INSERT INTO " + TwitterConstants.TABLENAME_ADDED_TWEETS + 
        " (uid,text,createdate) VALUES (?, ?, ?)"
    );
    
    public VoltTable[] run(long uid, String text, TimestampType timestamp) {
    	voltQueueSQL(insertTweet, uid, text, timestamp);
    	return voltExecuteSQL(true);
    }
}
