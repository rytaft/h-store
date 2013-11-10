/* Essam Mansour Tuple Tracker
 *
 */

#include "common/debuglog.h"
#include "common/FatalException.hpp"
#include "common/ValueFactory.hpp"
#include "storage/TupleTracker.h"
#include "storage/tablefactory.h"
#include <algorithm>
#include <cassert>

using namespace std;

namespace voltdb {


int64_t TupleTrackerManager::summedAccessFreq;
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

    isTupleTrackingInfoExtracted = false;

    summedAccessFreq = 0;
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

	    summedAccessFreq = summedAccessFreq + 1; // to report total access count per partition
	    access->frequency = access->frequency + 1;
	    access->by->insert(txnId);

	    //*/
}

void TupleTrackerManager::eraseTupleTrackingInfo(){
	/*/
	std::vector<TupleTrackingInfo>::iterator myIter=v_tupleTrackingInfo.begin();
	    while(myIter!=v_tupleTrackingInfo.end()) {
		delete *myIter;
		++myIter;
	    }
	//*/

	    v_tupleTrackingInfo.clear();

	    isTupleTrackingInfoExtracted = false;
}

bool TupleTrackerManager::sortPerPartition(const TupleTrackingInfo& first, const TupleTrackingInfo& second){

	    return (first.frequency > second.frequency);


   }


void TupleTrackerManager::sortTupleTrackingInfo(){

	std::sort(v_tupleTrackingInfo.begin(), v_tupleTrackingInfo.end() , sortPerPartition);

}
void TupleTrackerManager::extractTupleTrackingInfo(){

	if (isTupleTrackingInfoExtracted == true)
		eraseTupleTrackingInfo();

	TupleTrackingInfo *info = NULL;
	Map_TupleIdAccesses *m_tupIdAccesses = NULL;
	boost::unordered_map<uint32_t, Accesses*>::const_iterator tupIter;
	boost::unordered_map<std::string, Map_TupleIdAccesses*>::const_iterator tabIter = m_tableAccesses.begin();
	while (tabIter != m_tableAccesses.end()){
		m_tupIdAccesses = tabIter->second;
		tupIter = m_tupIdAccesses->begin();

		while (tupIter != m_tupIdAccesses->end()){
		info = new TupleTrackingInfo();
		info->tableName = tabIter->first;
		info->tupleID   = tupIter->first;
		info->frequency = tupIter->second->frequency;
		v_tupleTrackingInfo.push_back(*info);
		tupIter++;
		}//inner while

	  tabIter++;
	}//outer while

	isTupleTrackingInfoExtracted = true;

	sortTupleTrackingInfo();

}


void TupleTrackerManager::getTopKPerPart(int k){

	if (isTupleTrackingInfoExtracted == false)
		extractTupleTrackingInfo(); // extract and sort per partition


	ofstream SLfile; // site load file
	std::stringstream ss ;
	ss << "siteLoadPID_"<<partitionId<<".del" ;
	std::string fileName=ss.str();
	SLfile.open (fileName.c_str());
	SLfile <<partitionId<<"\t"+summedAccessFreq;
	SLfile.close();

	ofstream HTfile; // hot tuples file
	std::stringstream ss2 ;
	ss2 << "hotTuplesPID_"<<partitionId<<".del" ;
	fileName=ss2.str();
	HTfile.open (fileName.c_str());

    //header first line
	HTfile << " |Table Name";
	HTfile << " |Tuple ID";
	HTfile << " |Frequency|";
	HTfile << "\n";


	std::vector<TupleTrackingInfo>::const_iterator iter = v_tupleTrackingInfo.begin();

	// print top k in myfile1
	int i = 0;
	while (iter != v_tupleTrackingInfo.end() && i < k) {

		HTfile << iter->tableName<<"\t";
		HTfile << iter->tupleID<<"\t";
		HTfile << iter->frequency<<"\n";
		i++;
		iter++;
	}

	HTfile.close();

}

void TupleTrackerManager::TupleTrackerManager::print() {

	//simplePrint();
	getTopKPerPart(100);
}


void TupleTrackerManager::TupleTrackerManager::simplePrint() {
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
