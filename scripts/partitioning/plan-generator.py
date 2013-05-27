# coding: utf-8
tables = { 'warehouse':10, 'customer':1300, 'order':10 }
partitions = 4
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
    partitionranges[cur_partition] = [keyscovered,range_max]
    cur_partition+=1
    keyscovered=range_max
  plan_out[tablename] = partitionranges
print plan_out    
