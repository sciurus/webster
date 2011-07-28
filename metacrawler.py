#!/usr/bin/python

import crawler
import focuser

import logging
import optparse
import Pyro.core
import Queue
import thread
import threading


LOGGING_LEVELS = {'critical': logging.CRITICAL,
		'error': logging.ERROR,
		'warning': logging.WARNING,
		'info': logging.INFO,
		'debug': logging.DEBUG}

parser = optparse.OptionParser()
parser.add_option('-l', '--logging-level', help='Logging level')
parser.add_option('-t', '--thread-count', type='int', default=1, help='Number of threads')
parser.add_option('-c', '--classifier-count', type='int', default=1, help='Number of classifiers')
parser.add_option('-p', '--page-count', type='int', default=100, help='Number of pages to crawl before recreating a thread')
parser.add_option('-f', '--classifier-port', type='int', default=2000, help='Port to create first classifier on. Others will increment by 1.')
parser.add_option('-s', '--server', type='str', default='localhost', help='Address of WebsterDBServer')
(options, args) = parser.parse_args()

logging_level = LOGGING_LEVELS.get(options.logging_level, logging.NOTSET)
logging.basicConfig(level=logging_level)
logger = logging.getLogger('')
#formatter = logging.Formatter("%(relativeCreated)d %(thread)d - %(message)s")
#logger.setFormatter(formatter)

# yeah, classifer and focuser mean the same things

logger.info("Thread count is %d" % options.thread_count)
logger.info("Classifer count is %d" % options.classifier_count)
logger.info("Focuser port is %d" % options.classifier_port)

category = args[0]

classifier_queue = Queue.Queue(options.classifier_count)
sem = threading.Semaphore(options.thread_count)
master_lock = threading.Semaphore()

def run():
	try:
		logger.debug('Starting crawler')
		c = crawler.Webster( Pyro.core.getProxyForURI("PYROLOC://%s:4242/webster" % options.server), classifier_queue, master_lock=master_lock)
		c.crawl_pages(options.page_count)
	finally:
		sem.release()


for port in range(options.classifier_port, options.classifier_port + options.classifier_count):
	logger.info("Starting classifier on %s" % port)
	classifier_queue.put( focuser.WebsterClassifier(category, port=port) )


while True:
	try:
		sem.acquire()
		thread.start_new_thread( run,() )
	except:
		logger.warn('Error occured in thread')
