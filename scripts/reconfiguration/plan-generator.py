# coding: utf-8
# author: aelmore
import json
import argparse
 

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
  parser = argparse.ArgumentParser()
  parser.add_argument("-t","--type",dest="type", choices=["ycsb","tpcc"], required=True, help="Generate this type")
  parser.add_argument("-c","--change-type", dest="change_type", choices=["scale-down", "scale-up"], help="How to  evolve")
  parser.add_argument("-n","--num-phases",dest="phases", type=int, default=4, help="How many phases")
  parser.add_argument("-s","--size",dest="size", type=int, required=True, help="Partition key size")
  parser.add_argument("-p","--partitions",dest="partitions", type=str, required=True, help="Partitions size append, comma delimited" )

  args = parser.parse_args()
  
  TABLE_MAP = {
    "ycsb": "USERTABLE",
    "tpcc": "WAREHOUSE"
  }
  print args
  if args.change_type == "scale-down":
    raise Exception("not implemented")
  elif "," in args.partitions:  
    tables = { TABLE_MAP[args.type]: args.size }
    default_table = TABLE_MAP[args.type]
    phases = {  }
    for x, parts in enumerate(args.partitions.split(",")):
      phases[x] = int(parts)
  else:
    raise Exception("not implemented yet")
  plan_json = genPlanJSON(tables, phases, default_table)
  print str(plan_json)
