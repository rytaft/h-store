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

#include "migration/MigrationManager.h"
#include "common/types.h"
#include "common/debuglog.h"
#include "storage/table.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"

#include <string>
#include <vector>

#ifndef EXTRACT_STAT_ENABLED
#define EXTRACT_STAT_ENABLED
#endif

#ifdef EXTRACT_STAT_ENABLED
#include "boost/timer.hpp"
#endif

namespace voltdb
{
            
// -----------------------------------------
// MigrationManager Implementation 
// -----------------------------------------
    
MigrationManager::MigrationManager(ExecutorContext *executorContext, catalog::Database *catalogDatabase) :
    m_executorContext(executorContext),
    m_catalogDatabase(catalogDatabase) {
    #ifdef EXTRACT_STAT_ENABLED
    VOLT_INFO("EXTRACT_STAT_ENABLED");
    m_timingResults.clear();
    #endif

    // TODO: Precompute all of the stuff that we need for each table
    
    m_extractedTables.clear();
    m_extractedTableNames.clear();

    init(NULL);
}

MigrationManager::~MigrationManager() {
    // TODO
}

  
void MigrationManager::init(PersistentTable *table) {
  m_table = table;
  if(m_table == NULL) {
    m_partitionIndex = NULL;
    m_partitionColumnsIndexed = false;
    m_exactMatch = false;
    m_outputTable = NULL;
    m_partitionKeySchema = NULL;
    m_matchingIndexColsSchema = NULL;
    m_outTableSizeInBytes = 0; 
  }
  else {

    //Get the right index to use
    //TODO andy ae this should be cached on initialization. do tables exists? when should migration mgr be created? should it exist in dbcontext
    m_partitionColumns = m_table->partitionColumns();    
    if(m_partitionColumns.size() == 0) {
      m_partitionColumns.push_back(m_table->partitionColumn());
    }
    m_partitionIndex = getPartitionColumnsIndex(); 
    m_partitionKeySchema = createPartitionKeySchema(static_cast<int>(m_partitionColumns.size()));
    
    if(m_partitionIndex == NULL) {
      VOLT_DEBUG("partitionColumn is not indexed. partitionColumn[0]: %d",m_partitionColumns[0]);
      //TODO ae what do we do when we have no index for the partition colum?
      m_partitionColumnsIndexed = false;
      m_exactMatch = false;
      m_matchingIndexColsSchema = NULL;
    } else {
      VOLT_DEBUG("partitionColumn is indexed. partitionColumn[0]: %d",m_partitionColumns[0]);
      m_partitionColumnsIndexed = true;
      m_exactMatch = (m_matchingIndexCols == m_partitionColumns.size());
      m_matchingIndexColsSchema = createPartitionKeySchema(m_matchingIndexCols);
    }

    //TODO ae andy -> How many byes should we set this to? Below is just a silly guess
    m_outTableSizeInBytes = 1024; 

    //output table
    m_outputTable = reinterpret_cast<Table*>(TableFactory::getCopiedTempTable(m_table->databaseId(),
							   m_table->name(),m_table,&m_outTableSizeInBytes));
  }
    
  //Extract Limit 
  m_dataLimitReach = false;
  m_tuplesExtracted = 0;
    
#ifdef EXTRACT_STAT_ENABLED
  m_timer.restart();
  m_rowsExamined = 0;
#endif
}

TableTuple MigrationManager::initKeys(const TupleSchema* keySchema) {
  TableTuple keys(keySchema);
  keys.move(new char[keys.tupleLength()]);
  return keys;
}

const TupleSchema* MigrationManager::createPartitionKeySchema(int nCols) {
  if(nCols > m_partitionColumns.size()) {
    VOLT_ERROR("Partition key schema may not have more columns than there are table partition columns. nCols: %d, number of partition columns: %d", nCols, static_cast<int>(m_partitionColumns.size()));
    nCols = static_cast<int>(m_partitionColumns.size());
  }

  std::vector<ValueType> columnTypes(nCols);
  std::vector<int32_t> columnSizes(nCols);
  std::vector<bool> allowNull(nCols);
  for(int i = 0; i < nCols; i++) {
    columnTypes[i] = m_table->schema()->columnType(m_partitionColumns[i]);
    columnSizes[i] = static_cast<int32_t>(NValue::getTupleStorageSize(columnTypes[i]));
    allowNull[i] = true;
  }

  return TupleSchema::createTupleSchema(columnTypes, columnSizes, allowNull, false);
}

bool MigrationManager::inIndexRange(const TableTuple& tuple, const TableTuple& maxKeys) {
  TableTuple keys = initKeys(m_matchingIndexColsSchema);
  TableTuple maxKeysCmp = initKeys(m_matchingIndexColsSchema);
  for(int i = 0; i < m_matchingIndexCols; ++i) {
    keys.setNValue(i, tuple.getNValue(m_partitionColumns[i]));
    maxKeysCmp.setNValue(i, maxKeys.getNValue(i));
  }

  int compare = maxKeysCmp.compare(keys);
  if (compare > 0 || (!m_exactMatch && compare >= 0)) {
    return true;
  }

  return false;
}


bool MigrationManager::inRange(const TableTuple& tuple, const RangeMap& rangeMap) {
  TableTuple keys = initKeys(m_partitionKeySchema);
  for(int i = 0; i < m_partitionColumns.size(); ++i) {
    keys.setNValue(i, tuple.getNValue(m_partitionColumns[i]));
  }

  RangeMap::const_iterator it = rangeMap.upper_bound(keys);
  if(it == rangeMap.end() || ((it->second).compare(keys) > 0)) {
    it = rangeMap.lower_bound(keys);
  }
  if(it == rangeMap.end()) {
    return false;
  }

  TableTuple minKeys = it->second;
  TableTuple maxKeys = it->first;
    
  //Is the partitionColumn in the range between min inclusive and max exclusive
  if ((minKeys.compare(keys) <= 0) && ((maxKeys.compare(keys) > 0) || 
      ((minKeys.compare(maxKeys) == 0) && maxKeys.compare(keys) == 0))){
    return true;
  }

  return false;
}


bool MigrationManager::extractTuple(TableTuple& tuple) {
  //Have we reached our datalimit and found another tuple
  if (m_dataLimitReach == true){
    VOLT_DEBUG("more tuples with limit");
    return true;
  }

  if (!m_outputTable->insertTuple(tuple)) {
    VOLT_ERROR("Failed to insert tuple from table '%s' into  output table '%s'",m_table->name().c_str(),m_outputTable->name().c_str());
    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, "Failed to insert tuple");
  }
 
  if (!m_table->migrateTuple(tuple)){
    VOLT_ERROR("Error migrating tuple");
  } 
  
  //m_table->deleteTuple(tuple, true);
  m_extractedList.push(tuple.address());

  //Count if we have taken the max tuples
  if (++m_tuplesExtracted >= m_extractTupleLimit) {
    VOLT_DEBUG("tuple limit reached: %d", m_tuplesExtracted);
    m_dataLimitReach = true;
  }

  return false;
}

// Deletes tuples that have been migrated.  
// Important for ensuring correctness with repeated reconfigurations, especially when there are unique indexes.
void MigrationManager::cleanUp() {
  TableTuple tuple(m_table->schema());
  size_t sizeBefore = m_extractedList.size();
  int totalDeleted = 0;
  boost::timer cleanUpTimer;

  while (!m_extractedList.empty()) {
    tuple.move(m_extractedList.front());
    if (tuple.isMigrated()) {
      m_table->deleteTuple(tuple, true);
      ++totalDeleted;
    }
    m_extractedList.pop();
  }

  VOLT_INFO("Finished cleaning up.  Deleted %d of %d extracted tuples in %0.4f seconds", totalDeleted, sizeBefore, cleanUpTimer.elapsed());
}

bool MigrationManager::searchBTree(const RangeMap& rangeMap) {

  const TupleSchema* keySchema = m_partitionIndex->getKeySchema();

  for(RangeMap::const_iterator it=rangeMap.begin(); it!=rangeMap.end(); ++it) {
    TableTuple minKeys = it->second;
    TableTuple maxKeys = it->first;
    
    TableTuple searchKeys = initKeys(keySchema);
    for(int i = 0; i < m_matchingIndexCols; i++) {
      searchKeys.setNValue(i, minKeys.getNValue(i));
    }

    for(int i = m_matchingIndexCols; i < searchKeys.sizeInValues(); ++i) {
      searchKeys.setNValue(i, NValue::getNullValue(searchKeys.getType(i)));
    }

    m_partitionIndex->moveToKeyOrGreater(&searchKeys);    
    TableTuple tuple(m_table->schema());

    // look through each tuple at this key and see if it's in our range
    while(((!(tuple = m_partitionIndex->nextValueAtKey()).isNullTuple()) ||
	   (!(tuple = m_partitionIndex->nextValue()).isNullTuple())) && inIndexRange(tuple, maxKeys)) {
        if (tuple.isMigrated()){
          VOLT_DEBUG("Skipping migrated tuple %s", tuple.debug("").c_str());
          continue;
        }
#ifdef EXTRACT_STAT_ENABLED
      m_rowsExamined++;
#endif

        if(m_exactMatch || inRange(tuple, rangeMap)) {
          if(extractTuple(tuple)) {
            return true;
          }
        }
    }
  }

  return false;
}

bool MigrationManager::scanTable(const RangeMap& rangeMap) {
  //Iterate through results
  
  uint32_t current_tuple_id = 0;
  TableRange tableRange(m_table->name(),rangeMap);
  
  TableTuple tuple(m_table->schema());
  TupleList tupleList;
  if (m_tableCache.count(tableRange)){
      tupleList = m_tableCache[tableRange];
  }

  if (!tupleList.empty()) {
    VOLT_DEBUG("Found a cached list (%zu)",tupleList.size());
    //We have a cached list of tuples

    //Iterate through list of tuples to be migrated for this table
    while(!tupleList.empty()) {
      #ifdef EXTRACT_STAT_ENABLED
        m_rowsExamined++;
      #endif
      current_tuple_id = tupleList.front();
      tuple.move(m_table->dataPtrForTuple(current_tuple_id));
      //VOLT_INFO(" ** Indexed tuple %s", tuple.debug("").c_str());
        
      if(tuple.isToBeMigrated()){
        //VOLT_INFO("cached tuple is to extracted");
        //Extract this tuple
        if(extractTuple(tuple)){
            //VOLT_INFO("extracted cached tuple and stopping due to limit"); 
            //update tuple cahce
            m_tableCache[tableRange] = tupleList;
            return true;
        } else{
          //Remove tuple from cahce if we extracted
          tupleList.pop();
        }
      }
    }    
  }  
   
  //We either had no cache or iterated through entire cache, do one more table scan to make sure
  TableIterator iterator(m_table);
  bool moreData = false;
  VOLT_DEBUG("Scanning table");
  while (iterator.next(tuple)) {
    if (tuple.isMigrated() || tuple.isToBeMigrated()){
      continue;
    }
    #ifdef EXTRACT_STAT_ENABLED
      m_rowsExamined++;
    #endif

    if(inRange(tuple, rangeMap)) {       
      if(extractTuple(tuple)){
        //VOLT_INFO(" ** Indexing tuple %s", tuple.debug("").c_str());
        current_tuple_id =  m_table->getTupleID(tuple.address());
        if (!m_table->flagToMigrateTuple(tuple)){
          VOLT_ERROR("Error setting toMigrateFlag for tuple");
        }             
        tupleList.push(current_tuple_id);
        if (!moreData) moreData = true;
      }
      else {
        //VOLT_INFO("Extracted tuple %s", tuple.debug("").c_str());           
      }
    }  // end inRange
  } //end while
  
  //check if keepExtracting and tupleList is empty() might be able to return moreData = false
  m_tableCache[tableRange] = tupleList;  
  return moreData; 
}





void MigrationManager::getRangeMap(RangeMap& rangeMap, TableIterator& inputIterator, TableTuple& extractTuple) {

  int size = extractTuple.sizeInValues(); 
  while(inputIterator.next(extractTuple)) {
    TableTuple minKeys = initKeys(m_partitionKeySchema);
    TableTuple maxKeys = initKeys(m_partitionKeySchema);
    for(int i = 0, j = 0; i+3 < size && j < minKeys.sizeInValues(); i+=3, j++) {
      NValue minKey = extractTuple.getNValue(i+2);
      NValue maxKey = extractTuple.getNValue(i+3);
  
      minKeys.setNValue(j, minKey);
      maxKeys.setNValue(j, maxKey);
    }
    
    if(minKeys.compare(maxKeys) > 0) {
      //Min key should never be greater than maxKey
      throwFatalException("Max extract key is smaller than min key");
    }

    if(rangeMap.find(maxKeys) == rangeMap.end()) {
      // we have to key on max key rather than min key because of the way std::map::upper_bound() works
      rangeMap.insert(std::make_pair(maxKeys, minKeys)); 
    }

    VOLT_DEBUG("ExtractRange %s %s - %s ", m_table->name().c_str(),minKeys.debugNoHeader().c_str(),maxKeys.debugNoHeader().c_str() );
  }
}

// this version of extractRanges() is for composite key partitioning 
Table* MigrationManager::extractRanges(PersistentTable *table, TableIterator& inputIterator, TableTuple& extractTuple, int32_t requestToken, int32_t extractTupleLimit, bool& moreData)
{
  init(table);
  
  m_extractTupleLimit = extractTupleLimit;

  RangeMap rangeMap;
  getRangeMap(rangeMap, inputIterator, extractTuple);
  m_timer.restart();  
  //TODO ae andy -> on searching and checking for the max key condition
  // (cont) should we be using an expression or ok to just do  value check end value on iteration?
  //IF b-Tree
  try {
    if (m_partitionColumnsIndexed && m_partitionIndex->getScheme().type == BALANCED_TREE_INDEX){
      VOLT_INFO("Searching for ranges to extract with B-Tree index %s", m_partitionIndex->getName().c_str());
      moreData = searchBTree(rangeMap);
    }  
    else if (!m_partitionColumnsIndexed || m_partitionIndex->getScheme().type == HASH_TABLE_INDEX
	     ||  m_partitionIndex->getScheme().type == ARRAY_INDEX) {
      // TODO: if the maximum number of keys is small, we may want to actually use the hash index to find
      // the values rather than scanning the table
      VOLT_INFO("Searching for ranges to extract by scanning the table");
      moreData = scanTable(rangeMap);
    } else {
      VOLT_ERROR("Unsupported Index type");
      throwFatalException("Unsupported Index type %d",m_partitionIndex->getScheme().type );
    }    
  } catch (SerializableEEException &e) {
    return NULL; // failed to insert into output table
  } 

  // delete migrated tuples
  //cleanUp();
  
  VOLT_DEBUG("Tuples extracted: %d, Output Table %s",m_tuplesExtracted, m_outputTable->debug().c_str());
  //m_extractedTables[requestToken] = m_outputTable;
  //m_extractedTableNames[requestToken] = m_table->name();

#ifdef EXTRACT_STAT_ENABLED
  TableTuple globalMin = rangeMap.begin()->second;
  TableTuple globalMax = rangeMap.rbegin()->first;

  std::string extract_id = "Extract:"+m_table->name()+" Range:"+globalMin.debugNoHeader().c_str()+"-"+globalMax.debugNoHeader().c_str();
  double time_taken= m_timer.elapsed();
  m_timingResults[extract_id] = (int32_t)time_taken;
  VOLT_INFO("ExtractRange %s:%s %s - %s, RowsExamined:%d, RowsExtracted:%d, Selectivity %0.4f, TimeTaken: %0.4f ", m_table->name().c_str(),m_table->columnName(m_partitionColumns[0]).c_str(), 
            globalMin.debugNoHeader().c_str(),globalMax.debugNoHeader().c_str(), m_rowsExamined, m_tuplesExtracted, (m_tuplesExtracted/(double)m_rowsExamined), time_taken);        

#endif

  return m_outputTable;
}

TableIndex* MigrationManager::getPartitionColumnsIndex() {
    std::vector<TableIndex*> tableIndexes = m_table->allIndexes();
    
    TableIndex *bestIndex = NULL;
    int bestMatchingIndexCols = 0;

    for (int i = 0; i < m_table->indexCount(); ++i) {
        TableIndex *index = tableIndexes[i];
        
        VOLT_DEBUG("Index %s ", index->debug().c_str());

	if(index->getScheme().type == BALANCED_TREE_INDEX) { // tree index
	  for(int i = 0; i < index->getColumnIndices().size() && i < m_partitionColumns.size(); i++) {
	     if (index->getColumnIndices()[i] == m_partitionColumns[i]){
	       if(i+1 >= bestMatchingIndexCols) {
		 VOLT_DEBUG("Index %s matches", index->getName().c_str());
		 bestMatchingIndexCols = i+1;
		 bestIndex = index;
	       }
	     } else {
	       break;
	     }
	   }
	}
        else if (index->getColumnCount() == 1) { // hash or array index with one column
	  // Note: this check is only necessary for range partitioning in which we might not have
	  // the value for every partitioning column.  If we were to switch to hash partitioning, 
	  // then we know we'll always have the value for every one of the partitionColumns, so 
	  // the column count doesn't have to be 1
	  if (bestIndex == NULL && index->getColumnIndices()[0] == m_partitionColumns[0]){
	    VOLT_DEBUG("Index %s matches", index->getName().c_str());
	    bestMatchingIndexCols = 1;
	    bestIndex = index;
	  }	  
	}
    }

    if(bestIndex == NULL) {
      VOLT_INFO("No index matches");
    }
    else {
      VOLT_INFO("Best index %s has %d matching columns", bestIndex->getName().c_str(), bestMatchingIndexCols);
    }
    m_matchingIndexCols = bestMatchingIndexCols;
    return bestIndex;
}


bool MigrationManager::confirmExtractDelete(int32_t requestTokenId) {
    if(m_extractedTables.find(requestTokenId) == m_extractedTables.end()){
        VOLT_DEBUG("confirmExtractDelte requestTokenId was not found");
        return false;
    }
    else {
        VOLT_DEBUG("confirmExtractDelete for token %d", requestTokenId);
        Table* migratedData = m_extractedTables[requestTokenId];
        migratedData->deleteAllTuples(true);
        delete migratedData;        
        m_extractedTables.erase(requestTokenId);
        m_extractedTableNames.erase(requestTokenId);
        return true;
    }      
    return false;
}
bool MigrationManager::undoExtractDelete(int32_t requestTokenId) {
    throwFatalException("Undo delete not implemented yet");
    return false;
}

bool MigrationManager::cleanUp(PersistentTable *table, int32_t maxTuples) {
  init(table);
  TableTuple tuple(m_table->schema());
  size_t sizeBefore = m_extractedList.size();
  int totalDeleted = 0;

  while (!m_extractedList.empty() && totalDeleted < maxTuples) {
    tuple.move(m_extractedList.front());
    if (tuple.isMigrated()) {
      m_table->deleteTuple(tuple, true);
      ++totalDeleted;
    }
    m_extractedList.pop();
  }

  VOLT_INFO("Finished cleaning up.  Deleted %d of %d extracted tuples", totalDeleted, sizeBefore);

  return !m_extractedList.empty(); // is there more data?
}



}
