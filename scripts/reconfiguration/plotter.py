#!/usr/bin/env python
import matplotlib
matplotlib.use('Agg')
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
import re
from texGraphDefault import *

OPT_GRAPH_WIDTH = 1200
OPT_GRAPH_HEIGHT = 600
OPT_GRAPH_DPI = 100

INTERVAL_TYPE_MAP = {
  "tps": "THROUGHPUT",
  "lat": "LATENCY",
  "lat50": "LATENCY_50",
  "lat95": "LATENCY_95",
  "lat99": "LATENCY_99",
}

RESULTS_TYPE_MAP = {
  "tps": "TXNTOTALPERSECOND",
  "lat": "TOTALAVGLATENCY",
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
     
    parser.add_argument("-s","--show", dest="show", choices=["tps","lat","lat50","lat95","lat99","latall"], required=True, help="Show this data type")            
    
    group = parser.add_mutually_exclusive_group() 
    group.add_argument("-d","--dir", dest="dir", help="The directory to load files from")            
    group.add_argument("-f","--file", dest="file", help="The file to graph")            
    
    parser.add_argument("--filter", dest="filter", help="Filter OUT files containing these stings (comma delim)")
    
    parser.add_argument("--title", dest="title", help="title for the figure")
    parser.add_argument("--ylabel", dest="ylabel", help="label for the y-axis")
    parser.add_argument("--xlabel", dest="xlabel", help="label for the x-axis")
    return parser

def getReconfigEvents(hevent_log):
    events = []
    if not os.path.exists(hevent_log):
        return None
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
    if not reconfig_events:
        return 
    for event in reconfig_events:
      ts = event[0]
      #find the index last row that has a smaller physical TS
      _i = df[(df['TIMESTAMP'] <= ts ) ][-1:].index

      #if we have new event set, otherwise append      
      if df.RECONFIG[_i] == "":
         df.RECONFIG[_i] = event[1]
      else:
         df.RECONFIG[_i] = df.RECONFIG[_i] + "-" + event[1]

            
def plotGraph(args):

    if args.ylabel != None:
        plot.ylabel(args.ylabel)
    else:
        if "lat" in args.show:
            plot.ylabel("Latency(ms)")
        else:
            plot.ylabel(args.show)
    if args.xlabel != None:
        plot.xlabel(args.xlabel)
    else:
        plot.xlabel("Elapsed Time (s)")
    
    if args.title != None:
        plot.title(args.title)
    else:
        #add \n every 128 
        #via http://stackoverflow.com/questions/2657693/insert-a-newline-character-every-64-characters-using-python
        title = re.sub("(.{128})", "\\1\n", str(args), 0, re.DOTALL) 
        plot.title(title, fontsize=12)
    plot.legend(loc="best",prop={'size':12})

    if args.save:
        plot.savefig("%s.png" % args.save)
    if not args.no_display:
        plot.show()



## ==============================================
## plot aggregate results 
## ==============================================
def plotResults(args, files, ax):
    dfs = [ (d, pandas.DataFrame.from_csv(d,index_col=1)) for d in files if "results" in d]
    data = {}
    x = 0
    for (_file, df) in dfs:
        name = os.path.basename(_file).split("-results")[0] 
        base_name = name
        if args.recursive:
            name = "%s-%s" % (name, os.path.dirname(_file).rsplit(os.path.sep,1)[1])   
            base_name = name
        for show_var in args.show_vars:
            color = COLORS[x % len(COLORS)]
            linestyle = LINE_STYLES[x % len(LINE_STYLES)]
            if len(args.show_vars) > 1:
                name = "%s-%s" % (base_name,show_var)
            data[name] = df[RESULTS_TYPE_MAP[show_var]].values
            if args.reconfig:
                reconfig_events = getReconfigEvents(_file.replace("interval_res.csv", "hevent.log"))
                addReconfigEvent(df, reconfig_events)
                if len(df[df.RECONFIG.str.contains('TXN')]) == 1:
                    LOG.error("TODO annotate this has reconfig")
                else:
                    LOG.error("Multiple reconfig events not currently supported")
                 
            print df
            if args.type == "line":
                #plot the line with the same color 
                ax.plot(df.index, data[name], color=color,label=name,ls=linestyle, lw=2.0)
            elif args.type == "bar":
                ax.bar(x, data[name], color=color, label=name)
            x+=1 # FOR
    plotFrame = pandas.DataFrame(data=data)
    if args.type == "line" or args.type == "bar":
        pass
        #plotFrame.plot(ax=ax )
    elif args.type == "boxplot":
        plotFrame.boxplot(ax=ax, notch=1, sym='', vert=1, whis=1.5, 
              positions=None, widths=None, patch_artist=False, bootstrap = 5000)
    else:
        raise Exception("unsupported plot type : " + args.type)  

    plotGraph(args)

## ==============================================
## tsd
## ==============================================
def plotTSD(args, files, ax):
    dfs = [ (d, pandas.DataFrame.from_csv(d,index_col=1)) for d in files if "interval" in d]
    data = {}
    init_legend = "Reconfig Init"
    end_legend = "Reconfig End"
    x = 0
    for (_file, df) in dfs:
        name = os.path.basename(_file).split("-interval")[0] 
        base_name = name
        if args.recursive:
            name = "%s-%s" % (name, os.path.dirname(_file).rsplit(os.path.sep,1)[1])   
            base_name = name
        for show_var in args.show_vars:
            color = COLORS[x % len(COLORS)]
            linestyle = LINE_STYLES[x % len(LINE_STYLES)]
            if len(args.show_vars) > 1:
                name = "%s-%s" % (base_name,show_var)
            data[name] = df[INTERVAL_TYPE_MAP[show_var]].values
            if args.reconfig:
                reconfig_events = getReconfigEvents(_file.replace("interval_res.csv", "hevent.log"))
                if reconfig_events:
                    addReconfigEvent(df, reconfig_events)
                    if len(df[df.RECONFIG.str.contains('TXN')]) == 1:
                        ax.axvline(df[df.RECONFIG.str.contains('TXN')].index[0], color=color, lw=1.5, linestyle="--",label=init_legend)
                        if any(df.RECONFIG.str.contains('END')):
                            ax.axvline(df[df.RECONFIG.str.contains('END')].index[0], color=color, lw=1.5, linestyle=":",label=end_legend)
                            end_legend = None
                        else:
                            LOG.error("*****************************************")
                            LOG.error("*****************************************")
                            LOG.error(" NO END FOUND %s " % name)
                            LOG.error("*****************************************")
                            LOG.error("*****************************************")
            
                        init_legend = None
                    else:
                        LOG.error("Multiple reconfig events not currently supported")
            print name     
            print df
            if args.type == "line":
                #plot the line with the same color 
                ax.plot(df.index, data[name], color=color,label=name,ls=linestyle, lw=2.0)
            x+=1 # FOR
    plotFrame = pandas.DataFrame(data=data)
    if args.type == "line":
        pass
        #plotFrame.plot(ax=ax )
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
    
    print args
    if args.show == "latall":
        if args.tsd:
            keymap = INTERVAL_TYPE_MAP
        else:
            keymap = RESULTS_TYPE_MAP

        args.show_vars = [x for x in keymap if "lat" in x] 
    else:
        args.show_vars = [args.show]
    if args.tsd:
        plotTSD(args, files, ax)   
    else:
        plotResults(args, files, ax)
    #LOG.debug("Files to plot %s" % (files))

    
    
if __name__=="__main__":
    parser = getParser()
    args = parser.parse_args()
    
    print args
    files = []
    if args.dir:
        if args.recursive:
            for _dir, _subdir, _files in os.walk(args.dir):
                files.extend([os.path.join(_dir,x) for x in _files])
                
        else:
            files = [os.path.join(args.dir,x) for x in os.listdir(args.dir)]
    else:
        files.append(args.file)
    if args.filter:
        for filt in args.filter.split(","):
            files = [f for f in  files if filt not in f]
    LOG.info("Files : %s " % "\n".join(files) ) 
    
    plotter(args, files)
    

## MAIN
