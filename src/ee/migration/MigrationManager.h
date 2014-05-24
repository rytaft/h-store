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

struct RecursiveRangeMap {
  std::map<NValue,std::pair<NValue, RecursiveRangeMap>,NValue::ltNValue> r;
};


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
    Table* m_outputTable;
    int m_outTableSizeInBytes;

#ifdef EXTRACT_STAT_ENABLED
    boost::timer m_timer;
    int m_rowsExamined;
#endif

    TableIndex* getPartitionColumnsIndex();
    void init(PersistentTable *table); 
    TupleSchema* getKeySchema(TableTuple& sample);
    TupleSchema* getKeySchema(TableTuple& sample, int nCols);
    TableTuple initKeys(TupleSchema* keySchema);
    void setKeys(const RecursiveRangeMap& rangeMap, TableTuple& minKeys, TableTuple& maxKeys, int keyIndex);
    bool inRange(const TableTuple& tuple, const TableTuple& maxKeys);
    bool inRange(const TableTuple& tuple, const RecursiveRangeMap& rangeMap, int keyIndex);
    bool extractTuple(TableTuple& tuple);
    bool searchBTree(const RecursiveRangeMap& rangeMap, TableTuple& minKeys, TableTuple& maxKeys, int keyIndex);
    bool scanTable(const RecursiveRangeMap& rangeMap);
    void getRecursiveRangeMap(RecursiveRangeMap& rangeMap, TableIterator& inputIterator, TableTuple& extractTuple);
}; // MigrationManager class


    
}

#endif
