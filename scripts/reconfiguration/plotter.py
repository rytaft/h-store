#!/usr/bin/env python

import os
import sys
import csv
import logging
import matplotlib.pyplot as plot
import pandas
import pylab
import logging
import getopt
import argparse
import numpy as np
from texGraphDefault import *

OPT_GRAPH_WIDTH = 1200
OPT_GRAPH_HEIGHT = 600
OPT_GRAPH_DPI = 100

TYPE_MAP = {
  "tps": "THROUGHPUT",
  "lat": "LATENCY"
}
## ==============================================
## LOGGING CONFIGURATION
## ==============================================

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
                                  datefmt='%m-%d-%Y %H:%M:%S')
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.DEBUG)



def getParser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t","--type", dest="type", choices=["cfd","boxplot","bar","line"], required=True, help="The type of graph to plot")
    
    parser.add_argument("--no-display", action="store_true", help="Do not display the graph")
    parser.add_argument("-v","--save", dest="save",  help="filename to save the generated plot")
   
    parser.add_argument("--tsd", action="store_true", help="Plot Time Series Data (intervals)")
    parser.add_argument("-r","--recursive", dest="recursive", action="store_true", help="Check directories recursively")
    parser.add_argument("--reconfig", dest="reconfig", action="store_true", help="Plot reconfig bars") 
     
    parser.add_argument("-s","--show", dest="show", choices=["tps","lat"], required=True, help="Show this data type")            
    
    group = parser.add_mutually_exclusive_group() 
    group.add_argument("-d","--dir", dest="dir", help="The directory to load files from")            
    group.add_argument("-f","--file", dest="file", help="The file to graph")            
    
    parser.add_argument("--filter", dest="filter", help="Filter OUT files containing these stings (comma delim)")
    
    parser.add_argument("--title", dest="title", help="title for the figure")
    parser.add_argument("--ylabel", dest="ylabel", help="label for the y-axis")
    parser.add_argument("--xlabel", dest="xlabel", help="label for the x-axis")

    return parser

def oldMain():
    base_dir = sys.argv[1]
    LOG.debug("base dir %s " % (base_dir))
    input_files = [x for x in os.listdir(base_dir) if "-interval" in x]
    LOG.debug(input_files)

    latencies = { }
    tps = { }
    for f in input_files:
        data = pandas.DataFrame.from_csv(os.path.join(base_dir,f))
        name=f.split("-interval")[0]
        latencies[name] = data.LATENCY.values
        tps[name] = data.THROUGHPUT.values

    #print latencies
    data.ELAPSED = data.ELAPSED /1000
    df = pandas.DataFrame(data=latencies, index=data.ELAPSED)
    #print df
    df.plot(style=['s--','o-','^-.','*-','-'],ms=5)
    plot.xlabel('Elapsed Time (seconds)')
    plot.ylabel('Latency (seconds)')
    plot.title('')
    #df.boxplot() 
    plot.show()


def getReconfigEvents(hevent_log):
    events = []
    with open(hevent_log, "r") as f:
        protocol = ''
        for line in f:
            items = line.split(",")
            ts = np.int64(items[0])    
            if "SYSPROC" in line:
                event = "TXN"
                protocol = items[len(items)-1].strip().split("=")[1]
            elif "INIT" in line:
                event = "INIT"
            elif "END" in line:
                event = "END"
            events.append((ts,event,protocol))
    return events

def addReconfigEvent(df, reconfig_events):
    df['RECONFIG'] = ''
    for event in reconfig_events:
      ts = event[0]
      #find the index last row that has a smaller physical TS
      _i = df[(df['TIMESTAMP'] <= ts ) ][-1:].index
      if df.RECONFIG[_i] == "":
         df.RECONFIG[_i] = event[1]
      else:
         df.RECONFIG[_i] = df.RECONFIG[_i] + "-" + event[1]

            
def plotGraph(args):



    if args.ylabel != None:
        plot.ylabel(args.ylabel)
    else:
        plot.ylabel(args.show)
    plot.xlabel(args.xlabel)
    plot.title(args.title)
    plot.legend(loc="best")

    if args.save:
        plot.savefig("%s.png" % args.save)
    if not args.no_display:
        plot.show()


## ==============================================
## tsd
## ==============================================
def plotTSD(args, files, ax):
    dfs = [ (d, pandas.DataFrame.from_csv(d)) for d in files if "interval" in d]
    data = {}
    colormap = {}
    LOG.error("CHECK AND ALIGN INTERVALS") #TODO
    for x,(_file, df) in enumerate(dfs):
        name = os.path.basename(_file).split("-interval")[0] 
        color = COLORS[x % len(COLORS)]
        colormap[name] = color   
        data[name] = df[TYPE_MAP[args.show]].values
        if args.reconfig:
            reconfig_events = getReconfigEvents(_file.replace("interval_res.csv", "hevent.log"))
            addReconfigEvent(df, reconfig_events)
            if len(df[df.RECONFIG.str.contains('TXN')]) == 1:
                ax.axvline(df[df.RECONFIG.str.contains('TXN')].index[0], color=color, lw=1.5, linestyle="--")
            else:
                LOG.error("Multiple reconfig events not currently supported")
        if args.type == "line":
            
            ax.plot(df.index, df[TYPE_MAP[args.show]], color=color,label=name, lw=2.0)
    plotFrame = pandas.DataFrame(data=data)
    if args.type == "line":
        pass
        #plotFrame.plot(ax=ax, colormap=colormap)
    elif args.type == "boxplot":
        plotFrame.boxplot(ax=ax)
    else:
        raise Exception("unsupported plot type for tsd : " + args.type)  

    plotGraph(args)
## ==============================================
## main
## ==============================================
def plotter(args, files):
    plot.figure()
    ax = plot.subplot(111)
    
    if args.tsd:
        plotTSD(args, files, ax)   
    #LOG.debug("Files to plot %s" % (files))

    
    
if __name__=="__main__":
    parser = getParser()
    args = parser.parse_args()
    
    print args
    files = []
    if args.dir:
        if args.recursive:
            for _dir, _subdir, _files in os.walk(args.dir):
                files = [os.path.join(_dir,x) for x in _files]
                
        else:
            files = [os.path.join(args.dir,x) for x in os.listdir(args.dir)]
    else:
        files.append(args.file)
    if args.filter:
        for filt in args.filter.split(","):
            files = [f for f in  files if filt not in f]
    LOG.info("Files : %s " % ",".join(files) ) 
    
    plotter(args, files)
    
    
def f2():     
    for scale, color in input_files:
        warehouse_scale = "%s" % scale
        label = " %s)" % warehouse_scale
        
        plot.clf()
        plot.xlabel('time')
        plot.ylabel('transactions per second')
        #ax = plot.gca()
        #ax.set_autoscaley_on(False)
        evictions = { }
        y_max = None



           
        with open("out/scale%s/tpcc-02p-interval_res.csv" % warehouse_scale, "r") as f:
            reader = csv.reader(f)
            
            col_xref = None
            x_values = [ ]
            y_values = [ ]
            row_idx = 0
            for row in reader:
                if col_xref is None:
                    col_xref = { }
                    for i in xrange(len(row)):
                        col_xref[row[i]] = i
                    continue
                row_idx += 1
                #if row_idx % 2 == 0: continue
                x_values.append(float(row[col_xref['INTERVAL']]))
                y_values.append(float(row[col_xref['LATENCY']]))
                
                y_max = max(y_max, y_values[-1])
                #if first and int(row[col_xref["EVICTING"]]) != 0:
                #    evictions[x_values[-1]] = int(row[col_xref["EVICTING"]]) / 1000.0
            ## FOR
            plot.plot(x_values, y_values, label=label, color=color)
        ## WITH

        '''
        if len(evictions) > 0:
            y_max = max(y_max, 120000)
            for x,width in evictions.iteritems():
                width = max(1, width)
                plot.vlines(x, 0, y_max, color='black', linewidth=width, linestyles='dashed', alpha=1.0)
        '''        
        ## IF
        
        F = pylab.gcf()
        #F.autofmt_xdate()

        # Now check everything with the defaults:
        size = F.get_size_inches()
        dpi = F.get_dpi()
        logging.debug("Current Size Inches: %s, DPI: %d" % (str(size), dpi))
        
        new_size = (OPT_GRAPH_WIDTH / float(dpi), OPT_GRAPH_HEIGHT / float(dpi))
        F.set_size_inches(new_size)
        F.set_dpi(OPT_GRAPH_DPI)
        
        # Now check everything with the defaults:
        size = F.get_size_inches()
        dpi = F.get_dpi()
        logging.debug("Current Size Inches: %s, DPI: %d" % (str(size), dpi))
        
        new_size = (OPT_GRAPH_WIDTH / float(dpi), OPT_GRAPH_HEIGHT / float(dpi))
        F.set_size_inches(new_size)
        F.set_dpi(OPT_GRAPH_DPI)

        plot.legend(loc='upper right')
        plot.savefig("warehousescale-%s.png" % warehouse_scale)
## MAIN
