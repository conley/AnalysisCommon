import os
from datetime import datetime
import sys
import subprocess
from glob import glob
import argparse

"""
MRRunner is a class that runs MRJobs.
"""


parser = argparse.ArgumentParser(description='Enumerate values for a given field')
parser.add_argument('field', help = 'The name of the field to enumerate', 
                    default = 'domainName')
parser.add_argument('tld', help = 'The tld data to use', 
                    default = 'pro')
parser.add_argument('--test', type=int, default=0,
                   help='Whether to run in test mode. 0 for no, 1 for yes.')
parser.add_argument('--numred', type=int, default=50,
                    help='Number of reduce tasks')
parser.add_argument('--nummap', type=int, default=262,
                    help='Number of map tasks')

args = parser.parse_args()
scriptname = os.path.basename(__file__)
NUMRED = args.numred
NUMMAP = args.nummap
FIELD = args.field
TLD = args.tld
MRfile = 'MREnumerateField.py'

maprfsoutdir = 'maprfs:///whois-cleanup/enumerate/' + TLD + '/' + FIELD
infile = '/mapr/whois/reprocessed/' + TLD + '/all.tab'

if __name__ == "__main__":
    print
    print '-- Starting %s at %s --' % (scriptname, str(datetime.now()),)
    print

    print '-- Enumerating field %s for whois %s data at %s --' % (
        FIELD, TLD, str(datetime.now()))

    if not args.test:
        input_maprfs_file = infile.replace('/mapr', 'maprfs://')
        #subprocess.call(['hadoop','fs','-rmr',maprfsoutdir + '/*'])
        mrjargs = [sys.executable, MRfile, 
                   '-r', 'hadoop', input_maprfs_file,
                   '--fieldname', FIELD,
                   '--jobconf', 'mapred.map.tasks='+str(NUMMAP),
                   '--jobconf', 'mapred.reduce.tasks='+str(NUMRED),
                   '-o', maprfsoutdir, '--no-output']
    else:
        mrjargs = [sys.executable, MRfile,
                   '--fieldname', FIELD,
                   '--runner', 'inline', 
                   '/home/zanli/johconle/rsynced/whois-cleanup/evaluate/test-data.dat']

    print 'About to call:'
    print ' '.join(mrjargs)
    print 'at', str(datetime.now())

    proc = subprocess.Popen(mrjargs, shell=False, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    stdout_value, stderr_value = proc.communicate()

    print
    print 'got stdout from running', MRfile
    print stdout_value
    print
    print
    print 'got stderr from running', MRfile
    print stderr_value
    print
    print '-- Done analyzing tld %s for field %s at %s' %(
        TLD, FIELD, str(datetime.now()))
    print
