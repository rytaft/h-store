import sys
import subprocess
import glob

dir = sys.argv[1]
root_table = sys.argv[2]

monitoring_files = glob.glob(dir + '/transactions-partition*.log')

for file in monitoring_files:
    subprocess.call('cp ' + dir + '/' + file + ' ' + dir + '/' + file + '.bak', shell=True)
    subprocess.call('grep \'' + root_table + '\\|END\' ' + dir + '/' + file + '.bak > ' + dir + '/' + file, shell=True)