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
#include "storage/ReadWriteTracker.h"

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
//typedef  boost::unordered_map<std::string, Map_TupleIdAccesses> Map_TableAccesses;


namespace voltdb {
    
class ExecutorContext;
class TableTuple;
class TupleSchema;
class Table;
    
/**
 * TupleTracker Manager for a single partition
 */
class TupleTrackerManager {
    public:
	     TupleTrackerManager(ExecutorContext *ctx,int32_t partId);
        ~TupleTrackerManager();
    
        void insertReadWriteTracker(ReadWriteTracker* rwtracker);
        void clear();
        
        void print();


    private:

        void insertTupleAccesses(boost::unordered_map<std::string, RowOffsets*> *map, int64_t txnId);
        void insertTuple(int64_t txnId, const std::string tableName, uint32_t tupleId);
        
        void getTuples(boost::unordered_map<std::string, RowOffsets*> *map) const;
        
        ExecutorContext *executorContext;
        TupleSchema *resultSchema;
        Table *resultTable;

        //tableName -> Map_TupleIdAccesses{<tupleID, Accesses>}
        boost::unordered_map<std::string, Map_TupleIdAccesses> m_tupleAccesses;

        int32_t partitionId;

}; // CLASS

}
#endif
