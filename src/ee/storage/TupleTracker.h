/* Essam Mansour Tuple Tracker
 */

#ifndef HSTORE_TUPLETRACKER_H
#define HSTORE_TUPLETRACKER_H

#include <string>
#include "boost/unordered_set.hpp"
#include "boost/unordered_map.hpp"
#include "common/tabletuple.h"
#include "common/TupleSchema.h"
#include "storage/table.h"

////del me later
#include <iostream>
#include <stdio.h>
#include <fstream>
#include <algorithm>
#include <cassert>
//using namespace std;

typedef boost::unordered_set<uint32_t> RowOffsets; //remove later

typedef boost::unordered_set<int64_t> TxnIDs;

typedef struct {
    	//int partitionId;
    	//int64_t txnId;
    	//std::string tableName;
    	//uint32_t tupleID;
    	int64_t accesses; // access frequency for a tuple
    	TxnIDs by; //set of txn accessed this tuple
    	} Accesses;

// tupleID > Accesses
typedef  boost::unordered_map<uint32_t, Accesses*> Map_TupleIdAccesses;

//tableName -> <tupleID, Accesses>
typedef  boost::unordered_map<std::string, Map_TupleIdAccesses> Map_TableAccesses;




namespace voltdb {
    
class ExecutorContext;
class TableTuple;
class TupleSchema;
class Table;
    
/**
 * Tuple Tracker for a single partition
 */
class TupleTracker {
    
    friend class TupleTrackerManager;
    
    public:
        TupleTracker(int32_t partId);
        ~TupleTracker();
        
        void markTupleRead(const std::string tableName, TableTuple *tuple);
        void markTupleWritten(const std::string tableName, TableTuple *tuple);
        
        void clear();
        
        std::vector<std::string> getTablesRead();
        std::vector<std::string> getTablesWritten();
        
    private:
        void insertTuple(boost::unordered_map<std::string, RowOffsets*> *map, const std::string tableName, TableTuple *tuple);
        std::vector<std::string> getTableNames(boost::unordered_map<std::string, RowOffsets*> *map) const;
        
        int32_t partitionId;
        
        // TableName -> RowOffsets
        boost::unordered_map<std::string, RowOffsets*> reads;
        boost::unordered_map<std::string, RowOffsets*> writes;
        
}; // CLASS

/**
 * TupleTracker Manager
 */
class TupleTrackerManager {
    public:
	     TupleTrackerManager(ExecutorContext *ctx);
        ~TupleTrackerManager();
    
        TupleTracker* enableTupleTracking(int32_t partitionId);
        TupleTracker* getTupleTracker(int32_t partitionId);
        void removeTupleTracker(int32_t partitionId);
        
        void print();

        Table* getTuplesRead(TupleTracker *tracker);
        Table* getTuplesWritten(TupleTracker *tracker);
        
    private:
        void getTuples(boost::unordered_map<std::string, RowOffsets*> *map) const;
        
        ExecutorContext *executorContext;
        TupleSchema *resultSchema;
        Table *resultTable;
        boost::unordered_map<int32_t, TupleTracker*> trackers;
}; // CLASS

}
#endif
