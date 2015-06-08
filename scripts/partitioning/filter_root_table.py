import sys
import subprocess
import glob

root_table = sys.argv[1]

monitoring_files = glob.glob('transactions-partition*.log')

for file in monitoring_files:
    subprocess.call('cp ' + file + ' ' + file + '.bak', shell=True)
    subprocess.call('grep \'' + root_table + '\\|END\' ' + file + '.bak > ' + file, shell=True)