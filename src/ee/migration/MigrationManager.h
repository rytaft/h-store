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


#ifndef MIGRATIONMANAGER_H
#define MIGRATIONMANAGER_H

#include "catalog/database.h"
#include "common/tabletuple.h"
#include "indexes/tableindex.h"
#include "storage/tableiterator.h"
#include <list>
#include <queue>

#ifndef EXTRACT_STAT_ENABLED
#define EXTRACT_STAT_ENABLED
#endif

#ifdef EXTRACT_STAT_ENABLED
#include "boost/timer.hpp"
#endif

namespace voltdb {

class Table;
class TableIndex;
class PersistentTable;
class ExecutorContext;


typedef std::queue<uint32_t> TupleList;
typedef std::map<TableTuple,TableTuple,TableTuple::ltTableTuple> RangeMap;


//typedef std::map<TableToRangeMap,TupleList,TableTuple::ltTableTuple> TupleCacheMap;
//typedef std::map<std::string, TupleCacheMap> TableCache;


class TableRange {
  public:
    std::string tableName;
    TableTuple minKey;
    TableTuple maxKey;
    
    
    TableRange(std::string _tableName, const RangeMap rangeMap){
      tableName = _tableName;
       for(RangeMap::const_iterator it=rangeMap.begin(); it!=rangeMap.end(); ++it) {
          minKey = it->second;
          maxKey = it->first;
       }
       //VOLT_INFO("TR %s %s - %s",  tableName.c_str(), minKey.debugNoHeader().c_str(),maxKey.debugNoHeader().c_str());
    }
    struct ltTableRange {
      bool operator()(const TableRange &v1, const TableRange &v2) const {
        //VOLT_INFO("Comparing %s %s-%s to %s %s-%s", v1.tableName.c_str(), v1.minKey.debugNoHeader().c_str(),v1.maxKey.debugNoHeader().c_str(),
          //        v2.tableName.c_str(), v2.minKey.debugNoHeader().c_str(),v2.maxKey.debugNoHeader().c_str());
        if (v1.tableName.compare(v2.tableName) == 0) {
          //Same tableName
          if (v1.minKey.compare(v2.minKey) == 0) {
            //same talbeName minKey
            return v1.maxKey.compare(v2.maxKey) < 0;
          } else {
            //same tableName different minKey
            return v1.minKey.compare(v2.minKey) < 0;
          }
        } else {
          //different tableName
          return v1.tableName.compare(v2.tableName) < 0;
        }
      }
    };
};

typedef std::map<TableRange, TupleList, TableRange::ltTableRange> TableCache;


class MigrationManager {
        
public: 
    MigrationManager(ExecutorContext *executorContext, catalog::Database *catalogDatabase);
    ~MigrationManager();

    /**
     * Extract a range from the given table
     * TODO: This is just a proposal and not what the real API should be...
     */
    Table* extractRanges(PersistentTable *table, TableIterator& inputIterator, TableTuple& extractTuple, int32_t requestTokenId, int32_t extractTupleLimit, bool& moreData);
    
    bool confirmExtractDelete(int32_t requestTokenId);
    bool undoExtractDelete(int32_t requestTokenId);
    
private:
    ExecutorContext *m_executorContext;
    catalog::Database *m_catalogDatabase;
    
    // map catalog reconfig/migration requestTokenIds to pointers of data tables that have been migrated
    std::map<int32_t, Table*> m_extractedTables;
    // map catalog reconfig/migration requestTokenIds to TableIds. used to undo a migration
    std::map<int32_t, std::string> m_extractedTableNames;
    std::map<std::string, int32_t> m_timingResults;
    
    // specific to a single extract, set in init()
    bool m_dataLimitReach;
    int m_tuplesExtracted;
    int32_t m_extractTupleLimit;
    PersistentTable *m_table;
    int m_matchingIndexCols;
    TableIndex* m_partitionIndex;
    std::vector<int> m_partitionColumns;
    bool m_partitionColumnsIndexed;
    bool m_exactMatch;
    Table* m_outputTable;
    int m_outTableSizeInBytes;
    const TupleSchema* m_partitionKeySchema;
    const TupleSchema* m_matchingIndexColsSchema;
    TableCache tableCache;
    
    bool m_markNotDelete;
    bool m_dynamicIndex;

#ifdef EXTRACT_STAT_ENABLED
    boost::timer m_timer;
    int m_rowsExamined;
#endif

    TableIndex* getPartitionColumnsIndex();
    void init(PersistentTable *table); 
    TableTuple initKeys(const TupleSchema* keySchema);
    const TupleSchema* createPartitionKeySchema(int nCols);
    bool inIndexRange(const TableTuple& tuple, const TableTuple& maxKeys);
    bool inRange(const TableTuple& tuple, const RangeMap& rangeMap);
    bool extractTuple(TableTuple& tuple);
    bool searchBTree(const RangeMap& rangeMap);
    bool scanTable(const RangeMap& rangeMap);
    void getRangeMap(RangeMap& rangeMap, TableIterator& inputIterator, TableTuple& extractTuple);
}; // MigrationManager class


    
}

#endif
