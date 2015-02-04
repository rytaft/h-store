import os
import shutil
import zipfile

import subprocess
import shlex

import sys

def plotResults(result_dir):
    print "Plotting",result_dir
    reconfig = " --reconfig"
    interval_file = [x for x in os.listdir(result_dir) if 'interval' in x]
    
    if len(interval_file) == 0:
        print "no interval file found"
        return
    interval_file = interval_file[0]
    exp_type = interval_file.rsplit('-',1)[0]
    
    base = "python plotter.py --tsd %s -d %s -s %s -t line --no-display -v %s %s"
    csv_base = "%s-results.csv" % exp_type
    csv = "--csv %s" % os.path.join(result_dir,csv_base)
    for show_type in ["tps", "lat50","lat","lat95"]:
        _file_base = exp_type
        _file_name = "%s-%s" % (_file_base, show_type)
        _file = os.path.join(result_dir, _file_name )
        _cmd = base % (reconfig, result_dir, show_type, _file, csv)
        print "Executing:\n\t %s"% _cmd
        _res = subprocess.call(shlex.split(_cmd))
        csv = ""
        if _res != 0:
            print "Error code %s when running %s" % (_res, _cmd)
                
                
workdir = '.'                              
if len(sys.argv) > 1:
    if os.path.isdir(sys.argv[1]):
        print "Setting dir to :", sys.argv[1]
        workdir = sys.argv[1]
else:
    print "using default direct"
    

files = [x for x in os.listdir(workdir) if '.zip' in x]
for f in files:
    print "Extracting ", f
    z = zipfile.ZipFile(os.path.join(workdir,f))
    z.extractall()
    tmp = os.path.join(os.listdir('out')[0])
    dst = os.path.join(workdir,tmp)
    print "Moving %s to local: \n %s -> %s " % (tmp,os.path.join('out',tmp),dst)
    shutil.move(os.path.join('out',tmp),dst)
    shutil.rmtree('out')
    #Remove Zip
    #os.remove(f)
    graphs = [x for x in os.listdir(dst) if 'tps' in x]
    if len(graphs) ==0:
        print "no graphs"
        plotResults(dst)
    #print os.listdir('.') 
    



