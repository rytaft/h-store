# coding: utf-8
from collections import Counter
import sys
import cPickle

if len(sys.argv) > 1:
    line_limit = int(sys.argv[1])
    print "set line limit %s" % line_limit
else:
    line_limit = None    
    print "no line limit"


following = Counter()
followers = Counter()
    
cnt = 0    
for line in open('twitter_rv.net'):
    usr, followr = line.strip().split('\t')
    following[followr] +=1
    followers[usr] +=1
    cnt+=1
    if line_limit and cnt > line_limit:
        print "Line limit reached : ",cnt
        break

print "Top Following %s " %  following.most_common(10)
print "Top Followers  %s " % followers.most_common(10)
res = {'followers' : followers , 'following': following}

file_name = 'count-%s.pik'%line_limit
cPickle.dump(res,open(file_name,'w'))
print "Read %s lines into %s " % (cnt, file_name)
