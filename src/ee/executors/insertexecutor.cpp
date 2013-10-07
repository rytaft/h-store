/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
/* Copyright (C) 2008 by H-Store Project
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

#include "insertexecutor.h"
#include "common/debuglog.h"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "plannodes/insertnode.h"
#include "execution/VoltDBEngine.h"
#include "storage/persistenttable.h"
#include "storage/streamedtable.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"
#include "storage/tablefactory.h"
#include "storage/temptable.h"
#include "common/types.h"
#include "storage/WindowTable.h"

// added by hawk, 10/4/2013
#include "catalog/catalogmap.h"
#include "catalog/table.h"
#include "catalog/column.h"
// ended by hawk

namespace voltdb {

bool InsertExecutor::p_init(AbstractPlanNode *abstract_node, const catalog::Database* catalog_db, int* tempTableMemoryInBytes) {
    VOLT_TRACE("init Insert Executor");

    m_node = dynamic_cast<InsertPlanNode*>(abstract_node);
    assert(m_node);
    assert(m_node->getTargetTable());
    assert(m_node->getInputTables().size() == 1);

    // create an output table that currently will contain copies of modified tuples
    m_node->setOutputTable(TableFactory::getCopiedTempTable(
            m_node->databaseId(),
            m_node->getInputTables()[0]->name(),
            m_node->getInputTables()[0],
            tempTableMemoryInBytes));

    // 2013-07-29 - PAVLO
    // Sail yo ho, kids. John Meehan and I were here, and we decided that in
    // order to support INSERT INTO..SELECT statements, we had to let the input
    // tables be any kind of table. This is because sometimes for certain SELECTS
    // you will get a handle to the original PersistentTable and not a TempTable.
    // We don't think that this should cause any problems.
    m_inputTable = m_node->getInputTables()[0];
    if (m_inputTable == NULL) {
        VOLT_ERROR("Missing input table for InsertPlanNode #%d [numInputs=%ld]",
        		   m_node->getPlanNodeId(), m_node->getInputTables().size());
        // VOLT_ERROR("INPUT TABLE DUMP:\n%s", m_node->getInputTables()[0]->debug().c_str());
        return false;
    }
    assert(m_inputTable);

    // Target table can be StreamedTable or PersistentTable and must not be NULL
    m_targetTable = m_node->getTargetTable();
    assert(m_targetTable);
    assert((m_targetTable == dynamic_cast<PersistentTable*>(m_targetTable)) ||
           (m_targetTable == dynamic_cast<StreamedTable*>(m_targetTable)));

    m_tuple = TableTuple(m_inputTable->schema());

	// added by hawk, 10/4/2013
	std::string targetTableName = m_targetTable->name();
    catalog::Table *targetTable = NULL;
    catalog::CatalogMap<catalog::Table> tables = catalog_db->tables();
    for ( catalog::CatalogMap<catalog::Table>::field_map_iter i = tables.begin(); i != tables.end(); i++) {
        catalog::Table *table = (*i).second;
        if (table->name().compare(targetTableName) == 0) {
            targetTable = table;
            break;
        }
    }
    assert(targetTable != NULL);

    catalog::CatalogMap<catalog::Column> columns = targetTable->columns();
	 /*
     * The first output column is the tuple address expression and it isn't part of our output so we skip
     * it when generating the map from input columns to the target table columns.
     */
	if (m_targetTable->name()=="votes_by_phone_number")
	{
		for(int ii = 1; ii <= columns.size(); ii++)
		{
			catalog::Column *column = columns.getAtRelativeIndex(ii-1);
			m_inputTargetMap.push_back(std::pair<int, int>( ii, column->index()));
		}

		m_inputTargetMapSize = (int)m_inputTargetMap.size();
	}
	m_targetTuple = TableTuple(m_targetTable->schema());
	// ended by hawk

    PersistentTable *persistentTarget = dynamic_cast<PersistentTable*>(m_targetTable);
    m_partitionColumn = -1;
    m_partitionColumnIsString = false;
    if (persistentTarget) {
        m_partitionColumn = persistentTarget->partitionColumn();
        if (m_partitionColumn != -1) {
            if (m_inputTable->schema()->columnType(m_partitionColumn) == voltdb::VALUE_TYPE_VARCHAR) {
                m_partitionColumnIsString = true;
            }
        }
        // TODO: Check if PersistentTable has triggers
        // TODO: If so, then set some flag in InsertExecutor to true
    }
    m_multiPartition = m_node->isMultiPartition();
    return true;
}

bool InsertExecutor::p_execute(const NValueArray &params, ReadWriteTracker *tracker) {
    assert(m_node == dynamic_cast<InsertPlanNode*>(abstract_node));
    assert(m_node);
    // XXX assert(m_inputTable == dynamic_cast<TempTable*>(m_node->getInputTables()[0]));
    assert(m_inputTable);
    assert(m_targetTable);
    VOLT_DEBUG("INPUT TABLE: %s\n", m_inputTable->debug().c_str());
#ifdef DEBUG
    //
    // This should probably just be a warning in the future when we are
    // running in a distributed cluster
    //
    if (m_inputTable->activeTupleCount() == 0) {
        VOLT_ERROR("No tuples were found in our input table '%s'",
                   m_inputTable->name().c_str());
        return false;
    }
#endif
    assert (m_inputTable->activeTupleCount() > 0);

    // count the number of successful inserts
    int modifiedTuples = 0;

    Table* outputTable = m_node->getOutputTable();
    assert(outputTable);

    //
    // An insert is quite simple really. We just loop through our m_inputTable
    // and insert any tuple that we find into our m_targetTable. It doesn't get any easier than that!
    //
    assert (m_tuple.sizeInValues() == m_inputTable->columnCount());
    assert(m_targetTuple.sizeInValues() == m_targetTable->columnCount()); // added by hawk, 10/4/2013

    TableIterator iterator(m_inputTable);
    while (iterator.next(m_tuple)) {
        VOLT_DEBUG("Inserting tuple '%s' into target table '%s'",
                   m_tuple.debug(m_targetTable->name()).c_str(), m_targetTable->name().c_str());
        VOLT_TRACE("Target Table %s: %s",
                   m_targetTable->name().c_str(), m_targetTable->schema()->debug().c_str());

        // if there is a partition column for the target table
        if (m_partitionColumn != -1) {

            // get the value for the partition column
            NValue value = m_tuple.getNValue(m_partitionColumn);
            bool isLocal = m_engine->isLocalSite(value);

            // if it doesn't map to this site
            if (!isLocal) {
                if (!m_multiPartition) {
                    VOLT_ERROR("Mispartitioned Tuple in single-partition plan.");
                    return false;
                }

                // don't insert
                continue;
            }
        }

		// modified by hawk, 10/4/2013
		// FIXME: such hard coded behavior should be replaced with normal way
		if (m_targetTable->name()=="votes_by_phone_number")
		{
	        VOLT_DEBUG("Hawk test updating votes_by_phone_number....... ");
	        VOLT_TRACE("Hawk test updating votes_by_phone_number....... ");

			void *target_address = m_tuple.getNValue(0).castAsAddress();
			m_targetTuple.move(target_address);
        
			// Read/Write Set Tracking
			if (tracker != NULL) {
				tracker->markTupleWritten(m_targetTable->name(), &m_targetTuple);
			}

			// Loop through INPUT_COL_IDX->TARGET_COL_IDX mapping and only update
			// the values that we need to. The key thing to note here is that we
			// grab a temp tuple that is a copy of the target tuple (i.e., the tuple
			// we want to update). This insures that if the input tuple is somehow
			// bringing garbage with it, we're only going to copy what we really
			// need to into the target tuple.
			//
			TableTuple &tempTuple = (dynamic_cast<PersistentTable*>(m_targetTable))->getTempTupleInlined(m_targetTuple);
			for (int map_ctr = 0; map_ctr < m_inputTargetMapSize; map_ctr++) {
				tempTuple.setNValue(m_inputTargetMap[map_ctr].second,
									m_tuple.getNValue(m_inputTargetMap[map_ctr].first));
			}

			// if there is a partition column for the target table
			if (m_partitionColumn != -1) {
				// check for partition problems
				// get the value for the partition column
				NValue value = tempTuple.getNValue(m_partitionColumn);
				bool isLocal = m_engine->isLocalSite(value);

				// if it doesn't map to this site
				if (!isLocal) {
					VOLT_ERROR("Hawk: mispartitioned tuple in single-partition plan for"
							   " table '%s'", m_targetTable->name().c_str());
					return false;
				}
			}

			if (!m_targetTable->updateTuple(tempTuple, m_targetTuple, true)) 
			{
				VOLT_INFO("Hawk: Failed to update tuple from table '%s'",
						  m_targetTable->name().c_str());
				return false;
			}

			modifiedTuples++;
		}
		else  // original part of insert behavior
		{
			// try to put the tuple into the target table
			if (!m_targetTable->insertTuple(m_tuple)) {
				VOLT_ERROR("Failed to insert tuple from input table '%s' into"
						   " target table '%s'",
						   m_inputTable->name().c_str(),
						   m_targetTable->name().c_str());
				return false;
			}

			// try to put the tuple into the output table
			if (!outputTable->insertTuple(m_tuple)) {
				VOLT_ERROR("Failed to insert tuple from input table '%s' into"
						   " output table '%s'",
						   m_inputTable->name().c_str(),
						   outputTable->name().c_str());
				return false;
			}

			// successfully inserted
			modifiedTuples++;
		}
		// ended by hawk

    }
    // Check if the target table is persistent, and if hasTriggers flag is true
    // If it is, then iterate through each one and pass in outputTable
    PersistentTable* persistTarget = dynamic_cast<PersistentTable*>(m_targetTable);
    if(persistTarget != NULL && persistTarget->hasTriggers()) {
    	std::vector<Trigger*>::iterator trig_iter;
    	for(trig_iter = persistTarget->getTriggers()->begin();
    			trig_iter != persistTarget->getTriggers()->end(); trig_iter++) {
    		//if statement to make sure the trigger is an insert... breaking
    		//if((*trig_iter)->getType() == (unsigned char)TRIGGER_INSERT)
    			(*trig_iter)->fire(m_engine, outputTable);
    	}
    }
    // Check if the target table is persistent, and if hasTriggers flag is true
    // If it is, then iterate through each one and pass in outputTable
    PersistentTable* persistTarget = dynamic_cast<PersistentTable*>(m_targetTable);
    if(persistTarget != NULL && persistTarget->hasTriggers()) {
    	std::vector<Trigger*>::iterator trig_iter;
    	for(trig_iter = persistTarget->getTriggers()->begin();
    			trig_iter != persistTarget->getTriggers()->end(); trig_iter++) {
    		//if statement to make sure the trigger is an insert... breaking
    		//if((*trig_iter)->getType() == (unsigned char)TRIGGER_INSERT)
    			(*trig_iter)->fire(m_engine, outputTable);
    	}
    }

    // add to the planfragments count of modified tuples
    m_engine->m_tuplesModified += modifiedTuples;
    VOLT_DEBUG("Finished inserting %d tuples", modifiedTuples);
    return true;
}

}
