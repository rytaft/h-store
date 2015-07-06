import random
import plan_generator


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
    "affinity-rand",
    "twitter-aff",
    "twitter-rand",
    "twitter-aff-t1",
    "twitter-aff-t2",
    "twitter-aff-t3",
    "twitter-aff-t4",
    "affinity-ms-sk2",
    "affinity-ms-sk3",
    "affinity-ms-sk4",
    "affinity-ms-sk5",
    "affinity-ms12",
    "affinity-test",

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

RAND_CLIENTS = [1,2,3,4]
RAND_CLIENT_THREADS = [1,10,20,40]
RAND_CLIENT_BLOCKING = [1,1,1,5,10,20]
RAND_PARTITIONS = [4,8,12,16,20,24,28,32]
RECONFIG_CLIENT_COUNT = 3
RAND_TWITTER_SIZES=[50,100,250,500]
RAND_AFF_SIZES=AFF_SIZES.keys()
aff_plan_dir_base = '../../plans/affinity/a'
twitter_plan_dir_base = '../../plans/twitter/t'
twitter_base = "%s-size%sk"
aff_base = "%s-size%s"

def genAll():
    for p in RAND_PARTITIONS:
      print p
      for t in RAND_TWITTER_SIZES:
        plan_base=twitter_base % (twitter_plan_dir_base, t)
        plan_path = '%s-%s.json' % (plan_base, p)
        print plan_path
        a2 = {}
        a2['type'] = 'twitter'
        a2['partitions'] = str(p)
        tsize = int(t) * 1000
        a2['twitter'] = "%s:%s:%s" % (tsize,tsize,tsize)
        plan_string = plan_generator.getPlanString(a2)
        with open(plan_path.replace("scripts/reconfiguration/",""),"w") as plan_w_file:
            plan_w_file.write("%s" % plan_string)
      for a in RAND_AFF_SIZES:
        plan_base = aff_base % (aff_plan_dir_base, a)
        plan_path = '%s-%s.json' % (plan_base, p)
        print plan_path
        _sz = AFF_SIZES[a]
        a2 = {}
        a2['type'] = 'affinity'
        a2['partitions'] = str(p)
        a2['affinity'] = "%s:%s:%s" % (_sz['suppliers'],_sz['products'],_sz['parts'])
        plan_string = plan_generator.getPlanString(a2)
        with open(plan_path.replace("scripts/reconfiguration/",""),"w") as plan_w_file:
            plan_w_file.write("%s" % plan_string)

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


    if 't1' in  args['exp_type']:
        fabric.env["client.threads_per_host"] = 80  # * partitions # min(50, int(partitions * 4))

    if 't2' in  args['exp_type']:
        fabric.env["client.threads_per_host"] = 160  # * partitions # min(50, int(partitions * 4))

    if 't3' in  args['exp_type']:
        fabric.env["client.threads_per_host"] = 40  # * partitions # min(50, int(partitions * 4))
        fabric.env["client.blocking_concurrent"] = 20 # * int(partitions/8)

    if 'rand' in args['exp_type']:
        fabric.env["elastic.imbalance_load"] = 0.15
        fabric.env["elastic.max_tuples_move"] = 1000
        #fabric.env["elastic."] =

    if 'twitter-rand' in args['exp_type']:
        fabric.env["client.threads_per_host"] = random.choice(RAND_CLIENT_THREADS)
        fabric.env["client.blocking_concurrent"] = random.choice(RAND_CLIENT_BLOCKING)
        fabric.env["client.count"] = random.choice(RAND_CLIENTS)




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

    if 'affinity-test' in args['exp_type']:
        fabric.env["client.threads_per_host"] = 1
        fabric.env["client.blocking_concurrent"] = 20
        fabric.env["client.count"] = 4
        fabric.env["benchmark.supplier_to_parts_offset"] = 0.0
        fabric.env["benchmark.product_to_parts_offset"] = 0.02
        fabric.env["benchmark.uses.is_random"] = False
        fabric.env["benchmark.supplies.is_random"] = False
        fabric.env["benchmark.product_to_parts_random_offset"] = False
        fabric.env["benchmark.supplier_to_parts_random_offset"] = False
        fabric.env["benchmark.max_parts_per_supplier"] = 500
        fabric.env["benchmark.max_parts_per_product"] = 20
        fabric.env["client.output_response_status"] = True
        fabric.env["output_response_status"] = True
        fabric.env["hstore.partitions_per_site"] = 6 #args['exp_type'].rsplit("-",1)[1]
        fabric.env["site.commandlog_enable"] = False
        fabric.env["site.status_enable"] = True

    if 'sk2' in args['exp_type']:
        fabric.env["benchmark.supplier_to_parts_offset"] = 0.2
    if 'sk3' in args['exp_type']:
        fabric.env["benchmark.supplier_to_parts_offset"] = 0.3
    if 'sk4' in args['exp_type']:
        fabric.env["benchmark.supplier_to_parts_offset"] = 0.4
    if 'sk5' in args['exp_type']:
        fabric.env["benchmark.supplier_to_parts_offset"] = 0.5

    if 'affinity-rand' in args['exp_type']:
        fabric.env["client.threads_per_host"] = random.choice(RAND_CLIENT_THREADS)
        fabric.env["client.blocking_concurrent"] = random.choice(RAND_CLIENT_BLOCKING)
        fabric.env["client.count"] = random.choice(RAND_CLIENTS)
        fabric.env["benchmark.supplier_to_parts_offset"] = random.choice([0.0, 0.1, 0.2, 0.3, 0.5])
        fabric.env["benchmark.product_to_parts_offset"] = random.choice([0.0, 0.0, 0.0, 0.01, 0.02, 0.10])
        fabric.env["benchmark.max_parts_per_supplier"] = random.choice([100,100,100,200,500,50,10])
        fabric.env["benchmark.max_parts_per_product"] = random.choice([10,10,10,20,50,5,2])


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
