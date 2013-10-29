RECONFIG_EXPERIMENTS = [
    "reconfig-baseline",
    "reconfig-wh-baseline-2",
    "reconfig-wh-baseline-3",
    "reconfig-wh-baseline-4",
    "reconfig-1",
    "stopcopy-1",
    "baseline-1",
]

def updateReconfigurationExperimentEnv(fabric, args, benchmark, partitions ):
    partitions_per_site = fabric.env["hstore.partitions_per_site"]
    if args['exp_type'] == 'reconfig-baseline':
        fabric.env["client.blocking_concurrent"] = 4 # * int(partitions/8)
        fabric.env["client.count"] = 8
        fabric.env["client.blocking"] = True
        fabric.env["client.output_response_status"] = True
        fabric.env["client.threads_per_host"] = min(50, int(partitions * 4))
    
    if 'reconfig-wh-baseline' in args['exp_type']:
        fabric.env["client.blocking_concurrent"] = 5 # * int(partitions/8)
        fabric.env["client.count"] = 8
        fabric.env["client.blocking"] = True
        fabric.env["client.output_response_status"] = True
        fabric.env["client.threads_per_host"] = min(50, int(partitions * 4))
        fabric.env["hstore.partitions_per_site"] = args['exp_type'].rsplit("-",1)[1] 
    
    if 'reconfig-1' in args['exp_type']:
        fabric.env["client.blocking_concurrent"] = 5 # * int(partitions/8)
        fabric.env["client.count"] = 8
        fabric.env["client.blocking"] = True
        fabric.env["client.output_response_status"] = True
        fabric.env["client.threads_per_host"] = min(50, int(partitions * 4))

    if 'stopcopy-1' in args['exp_type']:
        fabric.env["client.blocking_concurrent"] = 5 # * int(partitions/8)
        fabric.env["client.count"] = 8
        fabric.env["client.blocking"] = True
        fabric.env["client.output_response_status"] = True
        fabric.env["client.threads_per_host"] = min(50, int(partitions * 4))

    if 'baseline-1' in args['exp_type']:
        fabric.env["client.blocking_concurrent"] = 5 # * int(partitions/8)
        fabric.env["client.count"] = 8
        fabric.env["client.blocking"] = True
        fabric.env["client.output_response_status"] = True
        fabric.env["client.threads_per_host"] = min(50, int(partitions * 4))
