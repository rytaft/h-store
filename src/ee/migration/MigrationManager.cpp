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

#include <string>
#include <vector>

namespace voltdb
{
            
// -----------------------------------------
// MigrationManager Implementation 
// -----------------------------------------
    
MigrationManager::MigrationManager() {
    // TODO
}

MigrationManager::~MigrationManager() {
    // TODO
}


bool MigrationManager::extractRange(PersistentTable *table, const NValue minKey, const NValue maxKey) {
    VOLT_DEBUG("ExtractRange %s %s - %s ", table->name().c_str(),minKey.debug().c_str(),maxKey.debug().c_str() );
    
    //Get the right index to use
    //TODO ae this should be cached on initialization?
    TableIndex* partitionIndex = getPartitionColumnIndex(table);    
    if(partitionIndex == NULL){
        //TODO ae what do we do when we have no index for the partition colum?
        throwFatalException("Table %s partition column is not an index", table->name().c_str());
    }    
    TableTuple tuple(table->schema());
    std::vector<ValueType> keyColumnTypes(1, minKey.getValueType());
    std::vector<int32_t> keyColumnLengths(1, NValue::getTupleStorageSize(minKey.getValueType()));
    std::vector<bool> keyColumnAllowNull(1, true);
    TupleSchema* keySchema =
        TupleSchema::createTupleSchema(keyColumnTypes,
                                       keyColumnLengths,
                                       keyColumnAllowNull,
                                       true);
    TableTuple searchkey(keySchema);
    searchkey.move(new char[searchkey.tupleLength()]);
    searchkey.setNValue(0, minKey);
    bool found = partitionIndex->moveToKey(&searchkey);    
    if(found){
        VOLT_DEBUG("Found");
    }
    return true;
}

TableIndex* MigrationManager::getPartitionColumnIndex(PersistentTable *table) {
    int partitionColumn = table->partitionColumn();
    std::vector<TableIndex*> tableIndexes = table->allIndexes();
    
    for (int i = 0; i < table->indexCount(); ++i) {
        TableIndex *index = tableIndexes[i];
        
        VOLT_DEBUG("Index %s ", index->debug().c_str());
        //One column in this index
        if(index->getColumnCount() == 1 && index->isUniqueIndex()) {
            if (index->getColumnIndices()[0] == partitionColumn){
                VOLT_DEBUG("Index matches");
                return index;
            }
        }
    }
    return NULL;
}


}

