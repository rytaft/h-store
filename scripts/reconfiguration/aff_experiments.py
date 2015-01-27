RECONFIG_EXPERIMENTS = [
    "affinity-dyn",
]


AFF_SIZES = { 
  "xs": {
    "suppliers": 100,
    "products": 1000,
    "parts" : 10000
  },
  "s": {
    "suppliers": 1000,
    "products": 10000,
    "parts" : 100000
  }

}

RECONFIG_CLIENT_COUNT = 1

def updateReconfigurationExperimentEnv(fabric, args, benchmark, partitions ):
    partitions_per_site = fabric.env["hstore.partitions_per_site"]
    if 'affinity-dyn' in args['exp_type']:
        fabric.env["client.blocking_concurrent"] = 5 # * int(partitions/8)
        fabric.env["client.count"] = RECONFIG_CLIENT_COUNT
        fabric.env["client.blocking"] = True
        fabric.env["client.output_response_status"] = True
        fabric.env["client.threads_per_host"] = min(50, int(partitions * 4))
        fabric.env["hstore.partitions_per_site"] = 4 #args['exp_type'].rsplit("-",1)[1] 
