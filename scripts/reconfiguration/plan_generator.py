# coding: utf-8
# author: aelmore
import json
import argparse
 

def genPlanJSON(tables,phases,default_table, onebased=False, multi=False, fine=False):

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
  plan["partition_plan"]={}
  
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
        if onebased:   
            partitionranges[cur_partition] = "%s-%s"% (keyscovered+1,range_max+1)
        else:
            partitionranges[cur_partition] = "%s-%s"% (keyscovered,range_max)
        cur_partition+=1
        keyscovered=range_max
      plan_out[tablename] = {} 
      plan_out[tablename]["partitions"] = partitionranges
      if multi:
          plan_out["district"] = {}
          plan_out["district"]["partitions"] = partitionranges
          plan_out["stock"] = {}
          plan_out["stock"]["partitions"] = partitionranges
      elif fine:
          plan_out["district"] = {}
          plan_out["district"]["partitions"] = partitionranges
          plan_out["stock"] = {}
          plan_out["stock"]["partitions"] = partitionranges
          plan_out["orders"] = {}
          plan_out["orders"]["partitions"] = partitionranges
          plan_out["customer"] = {}
          plan_out["customer"]["partitions"] = partitionranges
    table_map ={}
    table_map["tables"]=plan_out
    plan["partition_plan"]=table_map
  
  plan_json = json.dumps(plan,indent=2)
  return plan_json



def getPlanString(args):
  TABLE_MAP = {
    "ycsb": "usertable",
    "tpcc": "warehouse",
    "affinity" : "suppliers",
    "twitter" : "user_profiles"
  }

  aff_size = None 
  if "affinity" in args and args["affinity"] and ":" in args["affinity"]:
    _t = args["affinity"].split(":")
    if len(_t) != 3:
        raise Exception("affinity size needs 3 values [Suppliers]:[Products]:[Parts] : %s " % args["affinity"])
    aff_size = {}
    aff_size["suppliers"] = int(_t[0])
    aff_size["products"] = int(_t[1])
    aff_size["parts"] = int(_t[2])
  elif args["type"] == "affinity":
    raise Exception("Set to affinity, but no affinity sizes %s" % args["affinity"])

  if "change_type" in args and args["change_type"] == "scale-down":
    raise Exception("not implemented")
  if "," in args["partitions"] or args["partitions"] != None:  
    if args["type"] != "affinity":
      tables = { TABLE_MAP[args.type]: args["size"] }
    else:
      tables =  aff_size
    default_table = TABLE_MAP[args["type"]]
    if "multi" in args and args["multi"]:
        default_table="district"
    phases = {  }
    for x, parts in enumerate(args["partitions"].split(",")):
      phases[x] = int(parts)
  else:
    raise Exception("not implemented yet")
  onebased = False
  if "type" in args and args["type"] == "tpcc":
      onebased = True
  multi = False
  fine = False
  if "multi" in args:
      multi = args["multi"]
  if "fine" in args:
      fine = args["fine"]
  
  plan_json = genPlanJSON(tables, phases, default_table, onebased,multi,fine)
  return str(plan_json)
  

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-t","--type",dest="type", choices=["ycsb","tpcc", "affinity","twitter"], required=True, help="Generate this type")
  parser.add_argument("-c","--change-type", dest="change_type", choices=["scale-down", "scale-up"], help="How to  evolve")
  parser.add_argument("-n","--num-phases",dest="phases", type=int, default=4, help="How many phases")
  parser.add_argument("-s","--size",dest="size", type=int, required=True, help="Partition key size")
  parser.add_argument("-a","--affinity", dest="affinity", default=None, help="Affinity Sizes [Suppliers]:[Products]:[Parts]")
  parser.add_argument("--twitter", dest="twitter", default=None, help="Twitter Sizes [Suppliers]:[Products]:[Parts]")
  parser.add_argument("-p","--partitions",dest="partitions", type=str, required=True, help="Partitions size append, comma delimited" )
  parser.add_argument("-m","--multi",dest="multi",  action="store_true", help="Gen multi col" )
  parser.add_argument("-f","--fine",dest="fine",  action="store_true", help="Gen fine multi col" )


  args = parser.parse_args()

  TABLE_MAP = {
    "ycsb": "usertable",
    "tpcc": "warehouse",
    "twitter" : "user_profiles",
    "affinity" : "suppliers"
  }

  aff_size = None 
  if args.affinity and ":" in args.affinity:
    _t = args.affinity.split(":")
    if len(_t) != 3:
        raise Exception("affinity size needs 3 values [Suppliers]:[Products]:[Parts] : %s " % args.affinity)
    aff_size = {}
    aff_size["suppliers"] = int(_t[0])
    aff_size["products"] = int(_t[1])
    aff_size["parts"] = int(_t[2])
  elif args.type == "affinity":
    raise Exception("Set to affinity, but no affinity sizes %s" % args.affinity)

  
  twt_size = None 
  if args.twitter and ":" in args.twitter:
    _t = args.twitter.split(":")
    if len(_t) != 3:
        raise Exception("twitte size needs 3 values [user_profiles]:[followers]:[follows] : %s " % args.twitter)
    twt_size = {}
    twt_size["user_profiles"] = int(_t[0])
    twt_size["followers"] = int(_t[1])
    twt_size["follows"] = int(_t[2])
  elif args.type == "affinity":
    raise Exception("Set to affinity, but no affinity sizes %s" % args.affinity)
  
  
  if args.change_type == "scale-down":
    raise Exception("not implemented")
  if "," in args.partitions or args.partitions != None:  
    
    if args.type == "affinity":
      tables =  aff_size
    elif args.type == "twitter":
      tables = twt_size 
    else:
      tables = { TABLE_MAP[args.type]: args.size }
    default_table = TABLE_MAP[args.type]
    if args.multi:
        default_table="district"
    phases = {  }
    for x, parts in enumerate(args.partitions.split(",")):
      phases[x] = int(parts)
  else:
    raise Exception("not implemented yet")
  onebased = False
  if args.type == "tpcc":
      onebased = True
  plan_json = genPlanJSON(tables, phases, default_table, onebased,args.multi,args.fine)
  print str(plan_json)
