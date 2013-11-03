/* Copyright (C) 2013 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "common/debuglog.h"
#include "common/FatalException.hpp"
#include "common/ValueFactory.hpp"
#include "storage/ReadWriteTracker.h"
#include "storage/tablefactory.h"

using namespace std;

namespace voltdb {

ReadWriteTracker::ReadWriteTracker(int64_t txnId,TupleTrackerInfo* tupleTracker,int32_t partId) :
        txnId(txnId) {
    
    // Let's get it on!
	tupleTrackerInfo = tupleTracker;
	partitionId = partId;

}

ReadWriteTracker::~ReadWriteTracker() {
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

void ReadWriteTracker::insertTuple(boost::unordered_map<std::string, RowOffsets*> *map, const std::string tableName, TableTuple *tuple) {
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
    VOLT_INFO("*** TXN #%ld -> %s / %d", this->txnId, tableName.c_str(), tupleId);


    tupleTrackerInfo->incrementAccesses(partitionId,tableName.c_str(),tupleId,1); //Essam

    //VOLT_INFO("*** Table %s : Tuple ID %d has Freq %ld", tableName.c_str(), tupleId, tuple->getTupleAccessFreq());//Essam
}

void ReadWriteTracker::markTupleRead(const std::string tableName, TableTuple *tuple) {
    this->insertTuple(&this->reads, tableName, tuple);
}

void ReadWriteTracker::markTupleWritten(const std::string tableName, TableTuple *tuple) {
    this->insertTuple(&this->writes, tableName, tuple);
}

std::vector<std::string> ReadWriteTracker::getTableNames(boost::unordered_map<std::string, RowOffsets*> *map) const {
    std::vector<std::string> tableNames;
    tableNames.reserve(map->size());
    boost::unordered_map<std::string, RowOffsets*>::const_iterator iter = map->begin();
    while (iter != map->end()) {
        tableNames.push_back(iter->first);
        iter++;
    } // FOR
    return (tableNames);
}

std::vector<std::string> ReadWriteTracker::getTablesRead() {
    return this->getTableNames(&this->reads);
}    
std::vector<std::string> ReadWriteTracker::getTablesWritten() {
    return this->getTableNames(&this->writes);
}

void ReadWriteTracker::clear() {
    this->reads.clear();
    this->writes.clear();
}

// -------------------------------------------------------------------------

//boost::unordered_map<int32_t, TupleTrackerInfo*> ReadWriteTrackerManager::tupleTrackers;

ReadWriteTrackerManager::ReadWriteTrackerManager(ExecutorContext *ctx) : executorContext(ctx) {
    CatalogId databaseId = 1;
    this->resultSchema = TupleSchema::createTrackerTupleSchema();
    
    string *resultColumnNames = new string[this->resultSchema->columnCount()];
    resultColumnNames[0] = std::string("TABLE_NAME");
    resultColumnNames[1] = std::string("TUPLE_ID"); 
    
    this->resultTable = reinterpret_cast<Table*>(voltdb::TableFactory::getTempTable(
                databaseId,
                std::string("ReadWriteTrackerManager"),
                this->resultSchema,
                resultColumnNames,
                NULL));




}

ReadWriteTrackerManager::~ReadWriteTrackerManager() {
    TupleSchema::freeTupleSchema(this->resultSchema);
    delete this->resultTable;
    
    boost::unordered_map<int64_t, ReadWriteTracker*>::const_iterator iter = this->trackers.begin();
    while (iter != this->trackers.end()) {
        delete iter->second;
        iter++;
    } // FOR

    //delete tupleTrackerInfo;//Essam
}

ReadWriteTracker* ReadWriteTrackerManager::enableTracking(int64_t txnId,int32_t partId) {
	partitionId = partId;

	TupleTrackerInfo* tupleTrackerInfo = NULL;
	// create a tupleTracker for the partition if it does not have
	boost::unordered_map<int32_t, TupleTrackerInfo*>::const_iterator iter;
	    iter = tupleTrackers.find(partId);
	    if (iter == tupleTrackers.end()) {
	    	tupleTrackerInfo = new TupleTrackerInfo(); //Essam
	    	//tupleTrackers[partId] = tupleTrackerInfo;//Essam
	    	tupleTrackers.insert(std::make_pair(partId,tupleTrackerInfo));
	    }



    ReadWriteTracker *tracker = new ReadWriteTracker(txnId,tupleTrackerInfo,partId);
    trackers[txnId] = tracker;

    return (tracker);
}

ReadWriteTracker* ReadWriteTrackerManager::getTracker(int64_t txnId) {
    boost::unordered_map<int64_t, ReadWriteTracker*>::const_iterator iter;
    iter = trackers.find(txnId);
    if (iter != trackers.end()) {
        return iter->second;
    }
    return (NULL);
}

//Essam
void ReadWriteTrackerManager::printTupleTrackers(){

	ofstream myfile1;
	myfile1.open ("printTupleTrackers.del");//Essam

	if(tupleTrackers.empty())
	{

		myfile1 << " TupleTrackers is empty";
		myfile1.close();
		return;

	}

	myfile1 << " TupleTrackers.size ="<<tupleTrackers.size();
	myfile1 << "\n";

   boost::unordered_map<int32_t, TupleTrackerInfo*>::const_iterator iter= tupleTrackers.begin();

	int i =0;
	while(iter != tupleTrackers.end()){
		if(iter->second!=NULL){
		 iter->second->printSortedInfo();//Essam print
			myfile1 << " TupleTracker["<<i<<"]\n";
			i++;
		}

		iter++;

	}

	 myfile1.close();
}

//Essam
TupleTrackerInfo* ReadWriteTrackerManager::getTupleTracker(int32_t partId) {
    boost::unordered_map<int32_t, TupleTrackerInfo*>::const_iterator iter;
    iter = tupleTrackers.find(partId);
    if (iter != tupleTrackers.end()) {
        return iter->second;
    }
    return (NULL);
}

//Essam
void ReadWriteTrackerManager::removeTupleTracker(int32_t partId) {

	/*
	TupleTrackerInfo *tupleTracker = this->getTupleTracker(partId);

	tupleTracker->printSortedInfo();//Essam print

    if (tupleTracker != NULL) {
    	delete tupleTracker;
    }
    //*/
}

///////////////////////
void ReadWriteTrackerManager::removeTracker(int64_t txnId) {
    ReadWriteTracker *tracker = this->getTracker(txnId);

    printTupleTrackers();//Essam

    if (tracker != NULL) {
        trackers.erase(txnId);
        delete tracker;
    }
}

void ReadWriteTrackerManager::getTuples(boost::unordered_map<std::string, RowOffsets*> *map) const {
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

Table* ReadWriteTrackerManager::getTuplesRead(ReadWriteTracker *tracker) {
    this->getTuples(&tracker->reads);
    return (this->resultTable);
}

Table* ReadWriteTrackerManager::getTuplesWritten(ReadWriteTracker *tracker) {
    this->getTuples(&tracker->writes);
    return (this->resultTable);
}

}
