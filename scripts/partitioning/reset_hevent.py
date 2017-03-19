import sys
import subprocess

dir = sys.argv[1]

procs = []
for ip in sys.argv[2:]:
#	p = subprocess.Popen('ssh ' + ip + ' \"tar -zcf ' + dir + '/monitoring-' + ip + '.tar.gz ' + dir + '/*-partition-*.log -P" ; scp -q ' + ip + ':' + dir + '/monitoring-' + ip + '.tar.gz ' + dir + '/. ; tar -xf ' + dir + '/monitoring-' + ip + '.tar.gz -C / -P', shell=True)
	p = subprocess.Popen('ssh ' + ip + ' \"rm ' + dir + '/hevent.log ; touch ' + dir + '/hevent.log\"', shell=True)
	procs.append(p)
[p.wait() for p in procs]
