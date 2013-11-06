/* Essam Mansour Tuple Tracker
 */

#include "common/debuglog.h"
#include "common/FatalException.hpp"
#include "common/ValueFactory.hpp"
#include "storage/TupleTracker.h"
#include "storage/tablefactory.h"

using namespace std;

namespace voltdb {

// -------------------------------------------------------------------------

TupleTrackerManager::TupleTrackerManager(ExecutorContext *ctx,int32_t partId) :
		executorContext(ctx),partitionId(partId) {
    CatalogId databaseId = 1;
    this->resultSchema = TupleSchema::createTrackerTupleSchema();
    
    string *resultColumnNames = new string[this->resultSchema->columnCount()];
    resultColumnNames[0] = std::string("TABLE_NAME");
    resultColumnNames[1] = std::string("TUPLE_ID"); 
    
    this->resultTable = reinterpret_cast<Table*>(voltdb::TableFactory::getTempTable(
                databaseId,
                std::string("TupleTrackerManager"),
                this->resultSchema,
                resultColumnNames,
                NULL));

}

TupleTrackerManager::~TupleTrackerManager() {
    TupleSchema::freeTupleSchema(this->resultSchema);
    delete this->resultTable;
}

void TupleTrackerManager::insertReadWriteTracker(ReadWriteTracker* rwtracker){
	int64_t txnId = rwtracker->getTxnId();
	boost::unordered_map<std::string, RowOffsets*> *reads = rwtracker->getReads();
	boost::unordered_map<std::string, RowOffsets*> *writes = rwtracker->getWrites();

	if(reads->empty()==false)
		insertTupleAccesses(reads,txnId);

	if(writes->empty()==false)
			insertTupleAccesses(writes,txnId);
}

void TupleTrackerManager::insertTupleAccesses(boost::unordered_map<std::string, RowOffsets*> *map, int64_t txnId){
	//*/
	RowOffsets *offsets = NULL;
	std::string tableName;

	//tablename -> set of tuple ids
	boost::unordered_map<std::string, RowOffsets*>::const_iterator iter = map->begin();

	while (iter != map->end()) {
		tableName = iter->first;
		offsets = iter->second;
        //*/
		//if(iter->second->empty()==false)
		//{
		  boost::unordered_set<uint32_t>::iterator tupleIdIter = offsets->begin();
		  while (tupleIdIter != offsets->end()) {
			insertTuple(txnId, tableName, *tupleIdIter);
			tupleIdIter++;
		  }
		//}
		//*/

		iter++;
     }
   //*/
}



void TupleTrackerManager::insertTuple(int64_t txnId, std::string tableName, uint32_t tupleId){

}

void TupleTrackerManager::TupleTrackerManager::print() {
	ofstream myfile1;
	std::stringstream ss ;
	ss << "TupleTrackerPID_"<<partitionId<<".del" ;
	std::string fileName=ss.str();
	myfile1.open (fileName.c_str());
	myfile1 << " welcome partition: "<<partitionId<<"\n";
	myfile1.close();
}


void TupleTrackerManager::getTuples(boost::unordered_map<std::string, RowOffsets*> *map) const {
    this->resultTable->deleteAllTuples(false);
    TableTuple tuple = this->resultTable->tempTuple();
    boost::unordered_map<std::string, RowOffsets*>::const_iterator iter = map->begin();
    while (iter != map->end()) {
        RowOffsets::const_iterator tupleIter = iter->second->begin();
        while (tupleIter != iter->second->end()) {
            int idx = 0;
            tuple.setNValue(idx++, ValueFactory::getStringValue(iter->first)); // TABLE_NAME
            tuple.setNValue(idx++, ValueFactory::getIntegerValue(*tupleIter)); // TUPLE_ID
            this->resultTable->insertTuple(tuple);
            tupleIter++;
        } // WHILE
        iter++;
    } // WHILE
    return;
}




// -------------------------------------------------------------------------

}
