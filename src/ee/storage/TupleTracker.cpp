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

	RowOffsets *offsets = NULL;
	std::string tableName;

	//tablename -> set of tuple ids
	boost::unordered_map<std::string, RowOffsets*>::const_iterator iter = map->begin();

	while (iter != map->end()) {
		tableName = iter->first;
		offsets = iter->second;
        boost::unordered_set<uint32_t>::iterator tupleIdIter = offsets->begin();
		while (tupleIdIter != offsets->end()) {
			insertTuple(txnId, tableName, *tupleIdIter);
			tupleIdIter++;
		}

		iter++;
     }

}



void TupleTrackerManager::insertTuple(int64_t txnId, std::string tableName, uint32_t tupleId){
	   //*/
	    Accesses* access = NULL;

	    Map_TupleIdAccesses *m_tupIdAccesses = NULL;
	    boost::unordered_map<uint32_t, Accesses*>::const_iterator tupIter;

	    boost::unordered_map<std::string, Map_TupleIdAccesses*>::const_iterator tabIter = m_tableAccesses.find(tableName);

	    if (tabIter != m_tableAccesses.end()) {//this table has accessed tuples
	    	m_tupIdAccesses = tabIter->second;
	    	tupIter = m_tupIdAccesses->find(tupleId);
	    	if (tupIter != m_tupIdAccesses->end()) { //tuple ID has a record of accesses
	    		access = tupIter->second;

	    	}
	    	else { // tuple ID does not exist: 1)create access struct 2) insert pair <tuplID,access>
	    		access = new Accesses();
	    		access->frequency = 0;
	    		access->by = new TxnIDs();
	    		m_tupIdAccesses->insert(std::make_pair(tupleId, access));
	    	}
	    } else {//this table does not exist: 1) create access struct 2) insert <tableName, Map_TupleIdAccesses()> 3) insert <tupleID, access>
	    	access = new Accesses();
	    	access->frequency = 0;
	    	access->by = new TxnIDs();
	    	m_tupIdAccesses = new Map_TupleIdAccesses();
	    	m_tableAccesses.insert(std::make_pair(tableName, m_tupIdAccesses));
	    	m_tupIdAccesses->insert(std::make_pair(tupleId, access));
	    }

	    access->frequency = access->frequency + 1;
	    access->by->insert(txnId);

	    //*/
}

void TupleTrackerManager::TupleTrackerManager::print() {
	ofstream myfile1;
	std::stringstream ss ;
	ss << "TupleTrackerPID_"<<partitionId<<".del" ;
	std::string fileName=ss.str();
	myfile1.open (fileName.c_str());
	myfile1 << " partition "<<partitionId<<" has "<<m_tableAccesses.size()<<" accessed tables.\n";
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
