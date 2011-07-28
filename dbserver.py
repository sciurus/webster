#!/usr/bin/python

import logging
import MySQLdb
import optparse
import Pyro.core
import random
import re
import sys
import time
import urlparse

class WebsterDBServer(Pyro.core.SynchronizedObjBase):
	def __init__(self,host,db):
		Pyro.core.SynchronizedObjBase.__init__(self)
		self.logger = logging.getLogger('')
		self.host = host
		self.db = db
		self.status = {'NOT_VISITED': 0, 'IN_PROGRESS': 1, "VISITED": 2, "VISIT_ERROR": 3, "CLASSIFY_ERROR": 4, "DECODE_ERROR": 5}
		self.max_depth = 2
		self.current_depth = 2
		self.cutoff = 1.0
		self.retry_limit = 3
		self.retries = 0
		self.id_by_hostname = {}
		self.pages_in_progress = []
		self.received_time = 0
		self.pages_received = 0

		logger.debug('Connecting to mysql')
		self.conn = MySQLdb.connect(host=self.host, db=self.db, user='user', passwd='password', charset='utf8')

  	def get_cursor(self):
		try:
			cursor = self.conn.cursor()
			cursor.execute('SELECT 1')
			return cursor
		except:
			self.logger.warn('Error in mysql connection. Retrying.')
			self.conn = MySQLdb.connect(host=self.host, db=self.db, user='user', passwd='password', charset='utf8')
			return self.get_cursor()

	def get_page(self):
		"""Gets a page to process"""
		cursor = self.get_cursor()
		# crawl must run in order by depth (or id, which should also order by depth) to get correct results
		if not self.pages_in_progress:
			ppm = self.pages_received / ( (time.time() - self.received_time) / 60.0)
			self.logger.info("Crawling at %f pages per minute" % ppm)
			if not cursor.execute("SELECT id, url FROM pages WHERE status = %s AND depth = %s LIMIT 1000", (self.status['NOT_VISITED'], self.current_depth)):
				# if we didnt get a page because theyve all been visited, retry any with errors
				if self.retries < self.retry_limit:
					self.retries += 1
					self.logger.info("Preparing retry %d for pages at depth %d with errors" % (self.retries, self.current_depth))
					cursor.execute("UPDATE pages SET status = %s WHERE status = %s and depth = %s", (self.status['NOT_VISITED'], self.status['VISIT_ERROR'], self.current_depth))
					cursor.execute("UPDATE pages SET status = %s WHERE status = %s and depth = %s", (self.status['NOT_VISITED'], self.status['CLASSIFY_ERROR'], self.current_depth))
					return self.get_page()
				# if we didn't get a page because there are no more at this depth, increase depth
				elif self.current_depth < self.max_depth:
					self.current_depth += 1
					self.logger.info("Switching to depth %d" % self.current_depth)
					# will want retries once done with this depth to
					self.retries = 0
					return self.get_page()
				# if were at max depth and through with retries, log it and return nothing
				else:
					self.logger.warn('No more unvisited pages in database')
					return None
			pages_tuple = cursor.fetchall()
			self.logger.debug("Got %d new pages from database" % cursor.rowcount)
			self.pages_received = cursor.rowcount
			self.received_time = time.time()
			for id, url in pages_tuple:
				cursor.execute("UPDATE pages SET status = %s WHERE id = %s", (self.status['IN_PROGRESS'], id))
			self.pages_in_progress = list(pages_tuple)
			# put the pages in random order to avoid hammering a single site or having all crawlers stall on a slow site
			random.shuffle(self.pages_in_progress)
		# get url from beginning of list, then remove that entry from list
		url = self.pages_in_progress[0][1]
		self.pages_in_progress = self.pages_in_progress[1:]
		return url

	def get_site_id(self, hostname):
		"""Return id of site, creating database entry if not present"""
		cursor = self.get_cursor()
		if hostname in self.id_by_hostname:
			id = self.id_by_hostname[hostname]
		else:
			if not cursor.execute("SELECT id FROM sites WHERE hostname = %s", (hostname,)):
				self.logger.debug("site %s was not in database" % hostname)
				cursor.execute("INSERT INTO sites (hostname) VALUES (%s)", (hostname,))
				cursor.execute("SELECT id FROM sites WHERE hostname = %s", (hostname,))
			id, = cursor.fetchone()
		self.logger.debug("site %s has id %d" % (hostname, id))
		return id
	
	def queue_page(self, url, queue_depth):
		"""Places new page in the database"""
		#assert type(url) is str
		#assert type(queue_depth) is int
		cursor = self.get_cursor()
		parsed = urlparse.urlparse(url)
		url = parsed.geturl()
		if parsed.scheme != 'http':
			# this happens rarely
			self.logger.debug("Will not queue not http page %s" % url)
			return False
		if not cursor.execute("SELECT url FROM pages WHERE url = %s",(url,)):
			try:
				site_id = self.get_site_id(parsed.netloc)
			except:
				self.logger.warn("Will not queue error getting site id for %s" % parsed.netloc)
				return False
			cursor.execute("INSERT INTO pages (site_id, depth, url) VALUES (%s, %s, %s)", (site_id, queue_depth, url))
			self.logger.debug("Queued with depth %d page %s" % (queue_depth, url))
			return True
		else:
			self.logger.debug("Will not queue already in database page %s" % url)
			return False

	def submit_page(self, url, relevance, unique_links=None, link_count=None, html=None):
		"""Updates the database with information about the page"""
		#assert type(url) is str
		#assert type(relevance) is float
		cursor = self.get_cursor()
		cursor.execute("SELECT id, site_id, depth FROM pages WHERE url=%s", (url,))
		(page_id, from_id, depth) = cursor.fetchone()
		self.logger.debug("page id %d is from %d and has depth %d" % (page_id, from_id, depth))
		cursor.execute("UPDATE pages SET status = %s, relevance = %s WHERE id = %s LIMIT 1", (self.status['VISITED'], relevance, page_id))
		self.logger.debug("Set visited with relevance %f page id %d" % (relevance, page_id))
		if relevance >= self.cutoff:
			#assert type(link_count) is dict
			#assert type(unique_links) is dict
			for hostname, count in link_count.iteritems():
				try:
					to_id = self.get_site_id(hostname)
				except:
					self.logger.warn("Will not insert links error getting site id for %s" % hostname)
					continue
				if not cursor.execute("SELECT count from links WHERE from_id = %s AND to_id = %s", (from_id, to_id)):
					self.logger.debug("%d links are first instance from %d to %d" % (count, from_id, to_id))
					cursor.execute("INSERT INTO links (from_id, to_id, count) VALUES (%s, %s, %s)", (from_id, to_id, count))				
				else:
					old_count, = cursor.fetchone()
					self.logger.debug("Already %d links from %d to %d" % (old_count, from_id, to_id))
					new_count = old_count + count
					cursor.execute("UPDATE links SET count = %s WHERE from_id = %s AND to_id = %s LIMIT 1", (new_count, from_id, to_id))
					self.logger.debug("Updated to %d links from %d to %d" % (new_count, from_id, to_id))
			if not html == None:
				cursor.execute("INSERT INTO page_text (page_id, html) VALUES (%s, %s)", (page_id, html))
				self.logger.debug("Inserted html")
			if depth < self.max_depth:
				self.logger.debug("Below max depth so queuing new pages")
				for link in unique_links.iterkeys():
					try:
						self.queue_page(link, depth+1)
					except:
						self.logger.warn("Error queueing page %s" % link)
		return True

	def submit_error(self, url, error):
		"""Updates the database with the error that occured when processing the page"""
		#assert type(url) is str
		#assert type(error) is int
		cursor = self.get_cursor()
		cursor.execute("UPDATE pages SET status = %s WHERE url = %s LIMIT 1", (error, url))
		self.logger.debug("Set error %d for page %s" % (error, url))
		return True
			

LOGGING_LEVELS = {'critical': logging.CRITICAL,
					'error': logging.ERROR,
					'warning': logging.WARNING,
					'info': logging.INFO,
					'debug': logging.DEBUG}
parser = optparse.OptionParser()
parser.add_option('-l', '--logging-level', help='Logging level')
parser.add_option('-a', '--bind-address', type='str', default='localhost', help='Address to listen on')
parser.add_option('-p', '--bind-port', type='int', default=4242, help='Port to listen on')
parser.add_option('-m', '--mysql-address', type='str', default='localhost', help='Address of MySQL server')
(options, args) = parser.parse_args()

logging_level = LOGGING_LEVELS.get(options.logging_level, logging.NOTSET)
logging.basicConfig(level=logging_level)
logger = logging.getLogger('')
category = args[0]

logger.debug('Setting up server')
Pyro.core.initServer()
daemon=Pyro.core.Daemon(host=options.bind_address, port=options.bind_port)
uri=daemon.connect(WebsterDBServer(host=options.mysql_address, db=category),"webster")

try:
	logger.info("Server is listening at %s" % uri)
	daemon.requestLoop()
except KeyboardInterrupt:
	logger.info('Server is shutting down')
	daemon.shutdown(True)




