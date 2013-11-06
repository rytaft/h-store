/* Essam Mansour Tuple Tracker
 */

#include "common/debuglog.h"
#include "common/FatalException.hpp"
#include "common/ValueFactory.hpp"
#include "storage/TupleTracker.h"
#include "storage/tablefactory.h"

using namespace std;

namespace voltdb {

TupleTracker::TupleTracker(int32_t partId) :
		partitionId(partId) {
    
    // Let's get it on!
}

TupleTracker::~TupleTracker() {
    boost::unordered_map<std::string, RowOffsets*>::const_iterator iter;
    
    iter = this->reads.begin();
    while (iter != this->reads.end()) {
        delete iter->second;
        iter++;
    } // WHILE
    
    iter = this->writes.begin();
    while (iter != this->writes.end()) {
        delete iter->second;
        iter++;
    } // WHILE
}

void TupleTracker::insertTuple(boost::unordered_map<std::string, RowOffsets*> *map, const std::string tableName, TableTuple *tuple) {
    RowOffsets *offsets = NULL;
    boost::unordered_map<std::string, RowOffsets*>::const_iterator iter = map->find(tableName);
    if (iter != map->end()) {
        offsets = iter->second;
    } else {
        offsets = new RowOffsets();
        map->insert(std::make_pair(tableName, offsets));
    }
    
    uint32_t tupleId = tuple->getTupleID();
    offsets->insert(tupleId);
    VOLT_INFO("*** TXN #%ld -> %s / %d", this->partitionId, tableName.c_str(), tupleId);
}

void TupleTracker::markTupleRead(const std::string tableName, TableTuple *tuple) {
    this->insertTuple(&this->reads, tableName, tuple);
}

void TupleTracker::markTupleWritten(const std::string tableName, TableTuple *tuple) {
    this->insertTuple(&this->writes, tableName, tuple);
}

std::vector<std::string> TupleTracker::getTableNames(boost::unordered_map<std::string, RowOffsets*> *map) const {
    std::vector<std::string> tableNames;
    tableNames.reserve(map->size());
    boost::unordered_map<std::string, RowOffsets*>::const_iterator iter = map->begin();
    while (iter != map->end()) {
        tableNames.push_back(iter->first);
        iter++;
    } // FOR
    return (tableNames);
}

std::vector<std::string> TupleTracker::getTablesRead() {
    return this->getTableNames(&this->reads);
}    
std::vector<std::string> TupleTracker::getTablesWritten() {
    return this->getTableNames(&this->writes);
}

void TupleTracker::clear() {
    this->reads.clear();
    this->writes.clear();
}

// -------------------------------------------------------------------------

TupleTrackerManager::TupleTrackerManager(ExecutorContext *ctx, , int32_t partId) :
		executorContext(ctx), partitionId(partId) {
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
    
    boost::unordered_map<int32_t, TupleTracker*>::const_iterator iter = this->trackers.begin();
    while (iter != this->trackers.end()) {
        delete iter->second;
        iter++;
    } // FOR
}

TupleTracker* TupleTrackerManager::enableTupleTracking(int32_t partitionId) {
    TupleTracker *tracker = new TupleTracker(partitionId);
    trackers[partitionId] = tracker;
    return (tracker);
}

TupleTracker* TupleTrackerManager::getTupleTracker(int32_t partitionId) {
    boost::unordered_map<int32_t, TupleTracker*>::const_iterator iter;
    iter = trackers.find(partitionId);
    if (iter != trackers.end()) {
        return iter->second;
    }
    return (NULL);
}


void TupleTrackerManager::print() {
	ofstream myfile1;
	std::stringstream ss ;
	boost::unordered_map<int32_t, TupleTracker*>::const_iterator iter;
	    iter = trackers.begin();
	    while (iter != trackers.end()) {

	    	ss << "TupleTrackerPID_"<<iter->first<<".del" ;
	    	std::string fileName=ss.str();
	    	myfile1.open (fileName.c_str());
	    	myfile1 << " welcome partition: "<<iter->first<<"\n";
	    	myfile1.close();
	    	iter++;
	    }

 }


void TupleTrackerManager::removeTupleTracker(int32_t partitionId) {
    TupleTracker *tracker = this->getTupleTracker(partitionId);
    if (tracker != NULL) {
        trackers.erase(partitionId);
        delete tracker;
    }
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

Table* TupleTrackerManager::getTuplesRead(TupleTracker *tracker) {
    this->getTuples(&tracker->reads);
    return (this->resultTable);
}

Table* TupleTrackerManager::getTuplesWritten(TupleTracker *tracker) {
    this->getTuples(&tracker->writes);
    return (this->resultTable);
}

}
