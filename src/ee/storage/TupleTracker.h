/* Essam Mansour Tuple Tracker
 *
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

//using namespace std;

typedef boost::unordered_set<uint32_t> RowOffsets; //remove later

typedef boost::unordered_set<int64_t> TxnIDs;

typedef struct {
    	//int partitionId;
    	//int64_t txnId;
    	std::string tableName;
    	//uint32_t tupleID;
    	int64_t tupleID; // for voter
    	int64_t frequency; // access frequency for a tuple
    	//TxnIDs* by; //set of txn accessed this tuple
    	} TupleTrackingInfo;

typedef struct {
    	//int partitionId;
    	//int64_t txnId;
    	//std::string tableName;
    	//uint32_t tupleID;
    	int64_t frequency; // access frequency for a tuple
    	TxnIDs* by; //set of txn accessed this tuple
    	} Accesses;

// tupleID > Accesses
//typedef  boost::unordered_map<uint32_t, Accesses*> Map_TupleIdAccesses;
typedef  boost::unordered_map<int64_t, Accesses*> Map_TupleIdAccesses;

//tableName -> <tupleID, Accesses>
//typedef  boost::unordered_map<std::string, Map_TupleIdAccesses> Map_TableAccesses;


namespace voltdb {

class VoltDBEngine;
class ExecutorContext;
class TableTuple;
class TupleSchema;
class Table;
class NValue;
    
/**
 * TupleTracker Manager for a single partition
 */
class TupleTrackerManager {
    public:
	     TupleTrackerManager(ExecutorContext *ctx,int32_t partId, VoltDBEngine* vEng);
        ~TupleTrackerManager();
    
        void insertReadWriteTracker(ReadWriteTracker* rwtracker);
        void clear();
        
        void print();


    private:
        void simplePrint();
        void getTopKPerPart(int k);
        void eraseTupleTrackingInfo();
        void extractTupleTrackingInfo();
        void sortTupleTrackingInfo();
        void insertTupleAccesses(boost::unordered_map<std::string, RowOffsets*> *map, int64_t txnId);
        void insertTuple(int64_t txnId, const std::string tableName, uint32_t tupleId);
        void getTuples(boost::unordered_map<std::string, RowOffsets*> *map) const;
        
        int64_t getPhoneNo(std::string tableName,uint32_t tupleId);// for voter int64_t
        int64_t getPrimaryKey(std::string tableName,uint32_t tupleId);
        

        //static int64_t summedAccessFreq;

        int64_t summedAccessFreq;

		static bool sortPerPartition(const TupleTrackingInfo& first, const TupleTrackingInfo& second);
       //static bool sortPerPartition(std::vector<TupleTrackingInfo>::iterator first, std::vector<TupleTrackingInfo>::iterator second);

        ExecutorContext *executorContext;
        TupleSchema *resultSchema;
        Table *resultTable;

        bool isTupleTrackingInfoExtracted;

        //tableName -> Map_TupleIdAccesses{<tupleID, Accesses>}
        boost::unordered_map<std::string, Map_TupleIdAccesses*> m_tableAccesses;

        //
        std::vector<TupleTrackingInfo> v_tupleTrackingInfo ; // tuple tracking info per partition

        int32_t partitionId;
        VoltDBEngine* voltDBEngine;

}; // CLASS

}
#endif
