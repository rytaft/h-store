# coding: utf-8
# author: aelmore
import json
 

def genPlanJSON(tables,phases,default_table):
  """
    A function to generate a partition plan json by evenly
    dividing up a the number of keys for a table by 
    the number of partitions.
    tables: map of { table name : num of keys }
    phases: map of { phase name : num of partitions }
    default_table : default table_name 
    The output is:
    {
      "partition_plans": { # fixed id
        "1": { # name of partition phase
          "tables": { # fixed id
            "warehouse": { # name of table
              "partitions": {  # fixed id
                "0": "0-3",  # partition_id: key range
                "1": "3-6",  # partition_id: key range 
                "2": "6-8"   # partition_id: key range
              }
            }
          }
        }
      }, 
      "default_table": "warehouse" #default table for partitioning
    }
  """
  plan = {}
  plan["default_table"]=default_table
  plan["partition_plans"]={}
  
  for phase_name,partitions in phases.iteritems():
    plan_out = {}
    for tablename, maxkey in tables.iteritems():
      rangesize = int(maxkey/partitions)
      keyscovered = 0
      partitionranges = {}
      cur_partition = 0
      filler = maxkey % partitions
      while keyscovered < maxkey:
        range_max = keyscovered+rangesize
        if filler != 0:
           range_max+=1
           filler-=1
        partitionranges[cur_partition] = "%s-%s"% (keyscovered,range_max)
        cur_partition+=1
        keyscovered=range_max
      plan_out[tablename] = {} 
      plan_out[tablename]["partitions"] = partitionranges
    table_map ={}
    table_map["tables"]=plan_out
    plan["partition_plans"][phase_name]=table_map
  
  plan_json = json.dumps(plan,indent=2)
  return plan_json


if __name__ == "__main__":
  tables = { 'WAREHOUSE': 2, 'ITEM':100000,  }
  default_table = "WAREHOUSE"
  phases = { "1":2, "2":1  }
  plan_json = genPlanJSON(tables, phases, default_table)
  print str(plan_json)
