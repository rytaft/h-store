#!/usr/bin/env python

import os
import sys
import csv
import logging
import matplotlib.pyplot as plot
import pandas
import pylab

OPT_GRAPH_WIDTH = 1200
OPT_GRAPH_HEIGHT = 600
OPT_GRAPH_DPI = 100

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    base_dir = sys.argv[1]
    print "base dir %s " % (base_dir)
    input_files = [x for x in os.listdir(base_dir) if "-interval" in x]
    print input_files

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
    plot.show()
    

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
