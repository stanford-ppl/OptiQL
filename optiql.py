#!/usr/local/bin/python

from optparse import OptionParser
import multiprocessing
import os, sys
import math
from socket import gethostname
from string import *
import ConfigParser
from time import localtime, strftime

data_dir_default = "/home/hchafi/projects/tpch"
java_home = "/home/kjbrown/java/jdk1.7.0_b147_x86_64"
optiql_home = "/home/hchafi/projects/OptiQL"

options = {}


def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose")
    parser.add_option("-f", "--flavor", dest="flavor", default="scala-seq", help="selects the flavor of optiql to run. Choices are [scala-seq (default)|scala-par]")
    parser.add_option("-s", "--scalefactor", dest="factor", default="1", help="picks the scale factor for the tpch benchmark. Choices are [0 (debug)|1|3|5]")
    parser.add_option("-d", "--datadir", dest="dir",default="_default", help="set the directory for tpch data")


    (opts, args) = parser.parse_args()
    if len(args) != 0:
        parser.error("incorrect number of arguments")
    
    loadOptions(opts)
    launchOptiQl(options)
    


def loadOptions(opts):
    options['verbose'] = opts.verbose

    if(opts.flavor =="scala-seq"):
        options['flavor']='scala-seq'
    elif(opts.flavor == "scala-par"):
        options['flavor']='scala-par'
    else:
        print "unrecognized flavor choice: " + opts.flavor
        exit(-1)
        
    f = opts.factor
    if(f == "0" or f == "1" or f == "3" or f == "5"):
        options['factor'] = f
    else:
        print "unrecognized factor choice: " + opts.factor

    if(opts.dir == "_default"):
        options['dir'] = data_dir_default

    print """
==============================
==   options
=============================="""
    for k in options.keys():
        print "options[" + k + "] = " + str(options[k])


def launchOptiQl(options):
    print """
==============================
==   Launching OptiQl
=============================="""
    os.putenv("JAVA_HOME", java_home)
    os.putenv("OQL_HOME", optiql_home)
    os.putenv("OQL_FACTOR", options['factor'])
    os.putenv("OQL_FLAVOR", options['flavor'])
    os.putenv("OQL_TPCH_DIR", options['dir'])
    os.system("bin/exec")


if __name__ == "__main__":
    main()
