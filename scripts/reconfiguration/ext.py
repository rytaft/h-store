import os
import shutil
import zipfile

import subprocess
import shlex
import pandas
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
    #csv = ""
    #for show_type in ["tps", "lat50","lat","lat95"]:
    for show_type in ["both"]:
        _file_base = exp_type
        _file_name = "%s-%s" % (_file_base, show_type)
        _file = os.path.join(result_dir, _file_name )
        _cmd = base % (reconfig, result_dir, show_type, _file, csv)
        print "Executing:\n\t %s"% _cmd
        _res = subprocess.call(shlex.split(_cmd))
        csv = ""
        if _res != 0:
            print "Error code %s when running %s" % (_res, _cmd)


def getDstDir(dst):
    i = 0
    base = dst
    while os.path.exists(dst):
        i+=1
        dst = "%s-%s" % (base,i)
    return dst


workdir = '.'
if len(sys.argv) > 1:
    if os.path.isdir(sys.argv[1]):
        print "Setting dir to :", sys.argv[1]
        workdir = sys.argv[1]
else:
    print "using default direct"



graph_dir = os.path.join(workdir,'graphs')

if not os.path.exists(graph_dir):
    os.mkdir(graph_dir)
print "graph dir", graph_dir
files = [x for x in os.listdir(workdir) if '.zip' in x]
lat_changes = []
over_100 = []
for f in files:
    print "Extracting ", f
    z = zipfile.ZipFile(os.path.join(workdir,f))
    use_out = False
    if 'out' in z.namelist():
        use_out = True

    if use_out:
        z.extractall()
        tmp = os.path.join(os.listdir('out')[0])
        dst = os.path.join(workdir,tmp)
        dst = getDstDir(dst)
        print "Moving %s to local: \n %s -> %s " % (tmp,os.path.join('out',tmp),dst)
        shutil.move(os.path.join('out',tmp),dst)
        shutil.rmtree('out')
    else:
        dst = z.filename.strip('.zip')
        dst = getDstDir(dst)
        z.extractall(path=dst)
    #Remove Zip
    #os.remove(f)
    graphs = [x for x in os.listdir(dst) if 'tps' in x]
    if len(graphs) ==0:
        print "no graphs"
        plotResults(dst)
    graph = [x for x in os.listdir(dst) if 'both.png' in x]
    for g in graph:
        a = os.path.join(dst,g)
        b = os.path.join(workdir,"%s.png"%dst)
        shutil.copyfile(a,b)
        shutil.move(b,graph_dir)
    #print os.listdir('.')
    results_file = [x for x in os.listdir(dst) if 'results.csv' in x]
    df = pandas.DataFrame.from_csv(os.path.join(dst,results_file[0]))[['LATENCY','THROUGHPUT']]
    start = df[:30].mean()
    end = df[-30:].mean()
    lat_per_change = (end.LATENCY - start.LATENCY)/start.LATENCY *100.0
    if lat_per_change > 100.0:
        over_100.append((lat_per_change,os.path.join(graph_dir, b) ))
    lat_changes.append(lat_per_change)
print lat_changes
print over_100
