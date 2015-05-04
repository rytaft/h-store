RECONFIG_EXPERIMENTS = [
    "affinity-dyn-b1000-t10000",
    "affinity-dyn-b1000-t1000",
    "affinity-dyn-b1000",
    "affinity-dyn-b500-t10000",
    "affinity-dyn-b500-t1000",
    "affinity-dyn-b500",
    "affinity-dyn-b100",
    "affinity-dyn-b10-t10000",
    "affinity-dyn-b10-t1000",
    "affinity-dyn-b10",
    "affinity-ms",
    "twitter",
    "affinity-ms-sk2",
    "affinity-ms-sk3",
    "affinity-ms-sk4",
    "affinity-ms-sk5",
    "affinity-ms12",

]


AFF_SIZES = { 
  "xs": {
    "suppliers": 100,
    "products": 1000,
    "parts" : 10000
  },
  "ss": {
    "suppliers": 300,
    "products": 3000,
    "parts" : 30000
  },
  "s": {
    "suppliers": 1000,
    "products": 10000,
    "parts" : 100000
  },
  "m": {
    "suppliers": 10000,
    "products": 100000,
    "parts" : 1000000
  },
  "l": {
    "suppliers": 100000,
    "products": 1000000,
    "parts" : 10000000
  },
  "z": {
    "suppliers": 10000000,
    "products" : 10000,
    "parts" :    10000000
  },
  "xs2": {
    "suppliers": 1000,
    "products": 100,
    "parts" : 10000
  },
  "s2": {
    "suppliers": 10000,
    "products": 1000,
    "parts" : 100000
  },
  "m2": {
    "suppliers": 100000,
    "products": 10000,
    "parts" : 1000000
  },
  "l2": {
    "suppliers": 1000000,
    "products": 100000,
    "parts" : 10000000
  },
}

RECONFIG_CLIENT_COUNT = 3

def updateReconfigurationExperimentEnv(fabric, args, benchmark, partitions ):
    partitions_per_site = fabric.env["hstore.partitions_per_site"]

    if 'twitter' in args['exp_type']:
        fabric.env["client.count"] = 1
        fabric.env["client.blocking"] = True
        fabric.env["client.output_response_status"] = True
        fabric.env["output_response_status"] = True
        fabric.env["client.threads_per_host"] = 40  # * partitions # min(50, int(partitions * 4))
        fabric.env["hstore.partitions_per_site"] =  12  #args['exp_type'].rsplit("-",1)[1] 
        fabric.env["site.commandlog_enable"] = False
        #fabric.env["client.txnrate"] = 1000
        fabric.env["client.blocking_concurrent"] = 5 # * int(partitions/8)

    if 'affinity-ms' in args['exp_type']:
        fabric.env["client.count"] = 1
        fabric.env["client.blocking"] = True
        fabric.env["benchmark.supplier_to_parts_offset"] = 0.0
        fabric.env["benchmark.uses.is_random"] = False
        fabric.env["benchmark.supplies.is_random"] = False
        fabric.env["benchmark.product_to_parts_random_offset"] = False
        fabric.env["benchmark.supplier_to_parts_random_offset"] = False
        fabric.env["benchmark.max_parts_per_supplier"] = 10
        fabric.env["benchmark.max_parts_per_product"] = 10
        fabric.env["client.output_response_status"] = True
        fabric.env["output_response_status"] = True
        fabric.env["client.threads_per_host"] = 40  # * partitions # min(50, int(partitions * 4))
        fabric.env["hstore.partitions_per_site"] = 8 #args['exp_type'].rsplit("-",1)[1] 
        fabric.env["site.commandlog_enable"] = False
        #fabric.env["client.txnrate"] = 1000
        fabric.env["client.blocking_concurrent"] = 5 # * int(partitions/8)

    if 'sk2' in args['exp_type']:
        fabric.env["benchmark.supplier_to_parts_offset"] = 0.2
    if 'sk3' in args['exp_type']:
        fabric.env["benchmark.supplier_to_parts_offset"] = 0.3
    if 'sk4' in args['exp_type']:
        fabric.env["benchmark.supplier_to_parts_offset"] = 0.4
    if 'sk5' in args['exp_type']:
        fabric.env["benchmark.supplier_to_parts_offset"] = 0.5


    if 'affinity-ms12' in args['exp_type']:
        fabric.env["hstore.partitions_per_site"] = 12 #args['exp_type'].rsplit("-",1)[1] 

    if 'affinity-dyn' in args['exp_type']:
        fabric.env["client.count"] = RECONFIG_CLIENT_COUNT
        fabric.env["client.blocking"] = True
        fabric.env["benchmark.supplier_to_parts_offset"] = 0.5
        fabric.env["benchmark.uses.is_random"] = False
        fabric.env["benchmark.supplies.is_random"] = False
        fabric.env["benchmark.product_to_parts_random_offset"] = False
        fabric.env["benchmark.supplier_to_parts_random_offset"] = False
        fabric.env["benchmark.max_parts_per_supplier"] = 100
        fabric.env["benchmark.max_parts_per_product"] = 10
        fabric.env["client.output_response_status"] = True
        fabric.env["output_response_status"] = True
        fabric.env["client.threads_per_host"] = 20  # * partitions # min(50, int(partitions * 4))
        fabric.env["hstore.partitions_per_site"] = 6 #args['exp_type'].rsplit("-",1)[1] 
        fabric.env["site.commandlog_enable"] = False
        if 'b1000' in args['exp_type']:
            fabric.env["client.blocking_concurrent"] = 1000 # * int(partitions/8)
        elif 'b500' in args['exp_type']:
            fabric.env["client.blocking_concurrent"] = 500 # * int(partitions/8)
        elif 'b100' in args['exp_type']:
            fabric.env["client.blocking_concurrent"] = 100 # * int(partitions/8)
        elif 'b10' in args['exp_type']:
            fabric.env["client.blocking_concurrent"] = 10 # * int(partitions/8)
        else:
            fabric.env["client.blocking_concurrent"] = 1 # * int(partitions/8)
            
        if 't10000' in args['exp_type']:
            fabric.env["client.txnrate"] = 10000 # * int(partitions/8)
        elif 't5000' in args['exp_type']:
            fabric.env["client.txnrate"] = 5000 # * int(partitions/8)
        elif 't1000' in args['exp_type']:
            fabric.env["client.txnrate"] = 1000 # * int(partitions/8)
        elif 't100' in args['exp_type']:
            fabric.env["client.txnrate"] = 100 # * int(partitions/8)
        else:
            pass
            #fabric.env["client.txnrate"] = 1 # * int(partitions/8)
            

