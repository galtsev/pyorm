#!/usr/bin/python2.4

import unittest
from test import test_memory_datasource, test_pg_datasource

def run_suite(cls):
    print "running suite %s" % str(cls)
    suite = unittest.makeSuite(cls)
    unittest.TextTestRunner(verbosity=2).run(suite)

def run():
    run_suite(test_memory_datasource.AllTests)
    run_suite(test_pg_datasource.AllTests)

run()

