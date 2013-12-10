import pandas
import matplotlib
import matplotlib.pyplot as plot
import pylab
import plotter
import os
from texGraphDefault import *

LOG = plotter.LOG

def plotTSD(files,filedir,var,ylabel,xlabel, ylim=None,xtrim=None, filename=None):
  data = []
  labels = []
  reconfigs = []
  dfs = []
  for f in files:
    _file = os.path.join(filedir,f[1])
    df = pandas.DataFrame.from_csv(_file,index_col=1)
    if xtrim:
      df = df[:xtrim]
    #print var
    #print df[var].values
    data.append(df[var].values)
    reconfig_events = plotter.getReconfigEvents(_file.replace("interval_res.csv", "hevent.log"))
    plotter.addReconfigEvent(df, reconfig_events)
    df.index = df.index / 1000
    reconfigs.append(df)
    labels.append(f[0])


  pylab.rc("axes", linewidth=2.0)
  pylab.rc("lines", markeredgewidth=2.0)


  f,axarr = plot.subplots(len(data), sharex=True, sharey=True)
  plot.title("", fontsize=16)
  plot.xlabel(xlabel,fontsize='16')
  #f.text(0.06, 0.5, 'common ylabel', ha='center', va='center', rotation='vertical', fontsize='18')
  
  pylab.ylim(ylim)

  #for tick in ax.yaxis.get_major_ticks():
  #  tick.label1.set_fontsize('16')
  #  tick.label2.set_fontsize('16')
  init_legend = "Reconfig Init"
  end_legend = "Reconfig End"
  color = "black"
  for j,(_d,r, name) in enumerate(zip(data,reconfigs,labels)):
    ax =axarr[j]
    if ylim:
      ax.set_ylim(ylim)
    ax.plot(df.index, _d, label=name)
    ax.set_ylabel(ylabel, fontsize='12')
    ax.set_title(name)
    if len(r[r.RECONFIG.str.contains('TXN')]) == 1:
      ax.axvline(r[r.RECONFIG.str.contains('TXN')].index[0], color=color, lw=1.5, linestyle="--",label=init_legend)
      if any(r.RECONFIG.str.contains('END')):
          ax.axvline(r[r.RECONFIG.str.contains('END')].index[0], color=color, lw=1.5, linestyle=":",label=end_legend)
          end_legend = None
      else:
          LOG.error("*****************************************")
          LOG.error("*****************************************")
          LOG.error(" NO END FOUND %s " % name)
          LOG.error("*****************************************")
          LOG.error("*****************************************")

      init_legend = None
    elif len(df[df.RECONFIG.str.contains('TXN')]) < 1:
        LOG.error("NO reconfig event found!")
    else:
        LOG.error("Multiple reconfig events not currently supported")  
   
  #labels = ax.set_xticklabels(labels , fontsize ='16')
  #labels = ax.set_yticklabels(['2000','2500','3000','3500','4000','4500'] , fontsize ='16')
  #labels = ax.get_xticklabels()
  #for label in ax.xaxis.get_ticklabels():
    #label.set_rotation(0)

  rcParams.update(params)
  plot.tight_layout()
  
  if filename:
    plot.savefig(filename, format = 'pdf')
  else:
    plot.show()

if __name__ == "__main__":

  ycsb_files = [ 
    ( "Stop and Copy YCSB (Zipfian)" , "stopcopy-ycsb-zipf/ycsb-08p-med4contract-interval_res.csv"),
    ( "Squall YCSB (Zipfian)" , "reconfig-ycsb-zipf/ycsb-08p-med4contract-interval_res.csv"),
    ( "Stop and Copy YCSB (Uniform)" , "stopcopy-ycsb-uniform/ycsb-08p-med4contract-interval_res.csv"), 
    ( "Squall YCSB (Uniform)" , "reconfig-ycsb-uniform/ycsb-08p-med4contract-interval_res.csv"), 
  ] 
  
  filedir = "/home/aelmore/out/ycsb-dist-med4/out"
  if not os.path.isdir(filedir):
    raise Exception("Not a directory")
  var = "LATENCY"
  ylabel = "Latency (ms)"
  xlabel = "Elapsed Time (seconds)"
  #plotTSD(ycsb_files,filedir,var, ylabel,xlabel, [0,50],180, "ycsbTSDmeanLat-8to4partitions-1gb")
  
  var = "THROUGHPUT"
  ylabel = "TPS"
  xlabel = "Elapsed Time (seconds)"
  plotTSD(ycsb_files,filedir,var, ylabel,xlabel, [0,15000],180, "ycsbTSDmeanTPS-8to4partitions-1gb" )  


