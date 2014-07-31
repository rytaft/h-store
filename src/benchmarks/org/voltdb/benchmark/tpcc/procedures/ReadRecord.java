/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Coded By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)   *								   
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

package org.voltdb.benchmark.tpcc.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
    partitionInfo = "WAREHOUSE.W_ID: 0",
    singlePartition = false
)
public class ReadRecord extends VoltProcedure {
	
	public final SQLStmt readWarehouse = new SQLStmt("SELECT * FROM WAREHOUSE WHERE W_ID=?");
    public final SQLStmt readDistrict = new SQLStmt("SELECT * FROM DISTRICT WHERE D_W_ID=?");
    public final SQLStmt readOrders = new SQLStmt("SELECT * FROM ORDERS WHERE O_W_ID=?");
    public final SQLStmt readNewOrder = new SQLStmt("SELECT * FROM NEW_ORDER WHERE NO_W_ID=?");
    public final SQLStmt readOrderLine = new SQLStmt("SELECT * FROM ORDER_LINE WHERE OL_W_ID=?");
    public final SQLStmt readStock = new SQLStmt("SELECT * FROM STOCK WHERE S_W_ID=?");
    public final SQLStmt readCustomer = new SQLStmt("SELECT * FROM CUSTOMER WHERE C_W_ID=?");
    public final SQLStmt readHistory = new SQLStmt("SELECT * FROM HISTORY WHERE H_W_ID=?");

    public VoltTable[] run(long id, String table) {
    	switch(table.toUpperCase()) {
    	default:
    	case "WAREHOUSE":
    		voltQueueSQL(readWarehouse, id); break;
    	case "DISTRICT":
    		voltQueueSQL(readDistrict, id); break;
    	case "ORDERS":
    		voltQueueSQL(readOrders, id); break;
    	case "NEW_ORDER":
    		voltQueueSQL(readNewOrder, id); break;
    	case "ORDER_LINE":
    		voltQueueSQL(readOrderLine, id); break;
    	case "STOCK":
    		voltQueueSQL(readStock, id); break;
    	case "CUSTOMER":
    		voltQueueSQL(readCustomer, id); break;
    	case "HISTORY":
    		voltQueueSQL(readHistory, id); break;
    	}
        return (voltExecuteSQL(true));
    }
}
