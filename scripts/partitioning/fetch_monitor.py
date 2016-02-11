import sys
import subprocess

dir = sys.argv[1]

if not ('localhost' in sys.argv[2:]):
    subprocess.call('rm ' + dir + '/transactions-partition*.log', shell=True)
    subprocess.call('rm ' + dir + '/interval-partition*.log', shell=True)
    subprocess.call('rm ' + dir + '/monitoring-*.tar.gz', shell=True)

procs = []
for ip in sys.argv[2:]:
	p = subprocess.Popen('ssh ' + ip + ' \"tar -zcf ' + dir + '/monitoring-' + ip + '.tar.gz ' + dir + '/*-partition-*.log -P" ; scp -q ' + ip + ':' + dir + '/monitoring-' + ip + '.tar.gz ' + dir + '/. ; tar -xf ' + dir + '/monitoring-' + ip + '.tar.gz -C / -P', shell=True)
	procs.append(p)
[p.wait() for p in procs]
