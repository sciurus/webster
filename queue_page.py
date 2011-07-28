#!/usr/bin/python

import Pyro.core
import sys

dest = Pyro.core.getProxyForURI("PYROLOC://localhost:4242/webster")


for page in sys.argv[1:]:
	if dest.queue_page(page, int(0)):
		print "Queued: %s" % page

