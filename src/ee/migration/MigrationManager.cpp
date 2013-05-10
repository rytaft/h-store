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


bool MigrationManager::extractRange(PersistentTable *table, ReferenceSerializeOutput m_resultOutput, const NValue minKey, const NValue maxKey) {
    VOLT_DEBUG("ExtractRange %s %s - %s ", table->name().c_str(),minKey.debug().c_str(),maxKey.debug().c_str() );
        
    //Get the right index to use
    //TODO ae this should be cached on initialization?
    TableIndex* partitionIndex = getPartitionColumnIndex(table);    
    int partitionColumn = table->partitionColumn();
    bool partitionColumnIsIndexed=true;
    if(partitionIndex == NULL){
        //TODO ae what do we do when we have no index for the partition colum?
        partitionColumnIsIndexed = false;
        //throwFatalException("Table %s partition column is not an index", table->name().c_str());
    }       
    
    TableTuple tuple(table->schema());
    //TODO ae andy -> How many byes should we set this to? Below is just a silly guess
    int outTableSizeInBytes = 1024; 
    //int outTableSizeInBytes = (maxKey.op_subtract(minKey)).castAs(VALUE_TYPE_INTEGER).getInteger() *tuple.maxExportSerializationSize();
    
    //output table
    Table* outputTable = reinterpret_cast<Table*>(TableFactory::getCopiedTempTable(table->databaseId(),
                    table->name(),
                    table,
                    &outTableSizeInBytes));
    
    //TODO ae andy -> should max go in search or be apart of expression for end on iteration?    
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
    
    //Do we have a single key to pull
    if(minKey.compare(maxKey)==0 && partitionColumnIsIndexed){
        bool found = partitionIndex->moveToKey(&searchkey);    
        if(found){
            VOLT_DEBUG("Found");
            if(!(tuple = partitionIndex->nextValueAtKey()).isNullTuple()){
                //TODO check if not migrated
                //TODO set as migrated or delete
                if (!outputTable->insertTuple(tuple))
                {
                    VOLT_ERROR("Failed to insert tuple from table '%s' into"
                            " output table '%s'",
                            table->name().c_str(),
                            outputTable->name().c_str());
                    return NULL;
                }
            } else{
                VOLT_ERROR("Tuple not found but index indicated it exists");
                return NULL;
            }            
        }
        else{
            VOLT_DEBUG("key not found for single key extract");       
            return NULL;
        }
    } else if(minKey.compare(maxKey)<0){
        
        //IF b-Tree
        if (partitionColumnIsIndexed && partitionIndex->getScheme().type == BALANCED_TREE_INDEX){
            
            //We have a range to check
            partitionIndex->moveToKeyOrGreater(&searchkey);    

            while((!(tuple = partitionIndex->nextValueAtKey()).isNullTuple()) ||
            (!(tuple = partitionIndex->nextValue()).isNullTuple()) ){
                
                //TODO ae andy -> should we be using an expression or ok to just do  value check
                // I do on iteration?
                VOLT_DEBUG(" -- %s",tuple.debugNoHeader().c_str());
            }
        }  // Else if hash index
        else if (!partitionColumnIsIndexed || partitionIndex->getScheme().type == HASH_TABLE_INDEX){
            //find key
            bool found = partitionIndex->moveToKey(&searchkey);    
            if (found){
                //TODO ae andy -> actually this may not do us any good, because the data is not stored in order
                //TODO ae andy -> if the range between keys is 'small' can we loop through possible keys and do hash look ups?
            } else{
                
            }
            
            //Iterate through results
            TableIterator iterator(table);
           
            while (iterator.next(tuple))
            {
                //Is the partitionColumn in the range between min inclusive and max exclusive
                if (minKey.compare(tuple.getNValue(partitionColumn)) <= 0 && maxKey.compare(tuple.getNValue(partitionColumn)) >0){
                    //TODO ae check if ! migrated?
                    if (!outputTable->insertTuple(tuple))
                    {
                        VOLT_ERROR("Failed to insert tuple from table '%s' into"
                                " output table '%s'",
                                table->name().c_str(),
                                outputTable->name().c_str());
                        return NULL;
                    }
                }
            }

        } else {            
            throwFatalException("Unsupported Index type %d",partitionIndex->getScheme().type );
        }
       
    
    } else {
        //Min key should never be greater than maxKey        
        //TODO ae andy -> Appropriate exception to throw?
        throwFatalException("Max key is smaller than min key");
    } 
    
    //TODO build right output location
    VOLT_DEBUG("Output Table %s",outputTable->debug().c_str());
    
    
    return outputTable;
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

