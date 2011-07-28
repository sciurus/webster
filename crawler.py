#!/usr/bin/python

import focuser

import chardet
import logging
import optparse
import Pyro.core
import Queue
import re
import robotparser
import socket
import time
import urllib2
import urlparse

class Webster:
	def __init__(self, master, classifier_queue, master_lock=None, store_text=False):
		self.logger = logging.getLogger('')
		self.master = master
		self.master_lock = master_lock
		self.classifier_queue = classifier_queue
		self.store_text=store_text
		self.status = {'NOT_VISITED': 0, 'IN_PROGRESS': 1, "VISITED": 2,
			"VISIT_ERROR": 3, "CLASSIFY_ERROR": 4, "DECODE_ERROR": 5}
		self.cutoff = 1.0
		self.delay = 15
		# this should match table definition in database
		self.max_url_length = 333
		self.charset_pattern = re.compile('["\']text/html; charset=(.*?)["\']', re.IGNORECASE)
		self.link_pattern = re.compile('href=["\'].*?["\']', re.IGNORECASE)
		self.exclude_pattern = re.compile('(\.css|\.ico|\.jpg|\.png|\.gif)', re.IGNORECASE)
		
		urllib2.UserAgent='Mozilla/5.0 (X11; U; Linux x86_64; en-US; rv:1.9.1.6) Gecko/20100107 Fedora/3.5.6-1.fc12 Firefox/3.5.6'
		socket.setdefaulttimeout(self.delay)

	def page_error(self, url, error):
		"""Wrapper to handle locking before submitting error"""
		if self.master_lock:
			self.master_lock.acquire()
		try:
			self.logger.debug("Submitting error %s for url %s" % (error, url))
			self.master.submit_error(url, error)
		except Exception,x:
			self.logger.error( ''.join(Pyro.util.getPyroTraceback(x)) )
		finally:
			if self.master_lock:
				self.master_lock.release()
		return True

	def page_success(self, url, relevance, unique_links=None, link_count=None, html=None):
		"""Wrapper to handle locking and soem other processing before submitting page"""
		if self.master_lock:
			self.master_lock.acquire()
		
		if self.store_text == True:
			html = html.encode('zlib').encode('base64')				

		try:
			self.logger.debug("Submitting page %s with relevance %f" % (url, relevance))
			# work around difficulty of sending None type over xmlrpc
			if relevance < self.cutoff:
				if self.store_text == False:
					self.master.submit_page(url, relevance)
				else:
					self.master.submit_page(url, relevance, html=html)
			else:
				if self.store_text == False:
					self.master.submit_page(url, relevance, unique_links, link_count)
				else:
					self.master.submit_page(url, relevance, unique_links, link_count, html)	
		except Exception,x:
			self.logger.error( ''.join(Pyro.util.getPyroTraceback(x)) )
		finally:
			if self.master_lock:
				self.master_lock.release()
		return True
		
	def crawl_page(self):
		"""Crawl a page"""
		if self.master_lock:
			self.master_lock.acquire()
		try:
			url = self.master.get_page()
			self.logger.debug("Got page %s" % url)
		except:
			self.logger.error("Could not communicate with dbserver")
			time.sleep(self.delay)
			return False
		finally:
			if self.master_lock:
				self.master_lock.release()

		if url == None:
			self.logger.warning("Crawler did not receive a page from master")
			time.sleep(self.delay)
			return False

		parsed = urlparse.urlparse(url)
		url = parsed.geturl()
		
		try:
			rp = robotparser.RobotFileParser()
			rp.set_url('http://' + parsed.netloc + '/robots.txt')
			self.logger.debug("Reading robots.txt")
			rp.read()
			fetchable = rp.can_fetch(urllib2.UserAgent, url)
		except:
			self.logger.debug("Processing robots.txt failed")
			self.page_error(url, self.status['VISIT_ERROR'])
			return True

		if not fetchable:
			self.logger.debug("Crawler is blocked by robots.txt")
			self.page_error(url, self.status['VISIT_ERROR'])
			return True

		try:
			self.logger.debug('Connecting to page')
			req = urllib2.Request(url)
			req.add_header("User-Agent", urllib2.UserAgent)
			opener = urllib2.build_opener()
			input = opener.open(req)
			opener.close()
		except:
			self.logger.debug("Connecting failed")
			self.page_error(url, self.status['VISIT_ERROR'])
			return True

		if input.headers.type != 'text/html':
			self.logger.debug('Page was not html')
			input.close()
			self.page_error(url, self.status['VISIT_ERROR'])
			return True

		try:
			self.logger.debug("Reading html")
			raw_html = input.read()
			input.close()
			self.logger.debug("Snippet of html is %s" % raw_html[:40])
		except:
			self.logger.debug("Reading html failed")
			self.page_error(url, self.status['VISIT_ERROR'])
			return True

		# avoid bad erros where python assumes page is ascii but its not

		charsets_to_try = []
		charset_identified = False

		try:
			ignore, charset_header = input.headers.getheader('content-type').split('charset=')
			charsets_to_try.append(charset_header)
			self.logger.debug("Character set specified in headers is %s" % charset_header)
		except:
			self.logger.debug('No character set specified in headers')

		charset_match = self.charset_pattern.search(raw_html)
		if charset_match:
			charset_meta = charset_match.group(1)
			charsets_to_try.append(charset_meta)
			self.logger.debug("Character set specified in meta tag is %s" % charset_meta)
		else:
			self.logger.debug('No character set specified in meta tag')

		for c in charsets_to_try:
			try:
				html = raw_html.decode(c)
				charset_identified = True
				break
			except:
				self.logger.debug("Character set %s was wrong" % c )
				continue

		if not charset_identified:
			self.logger.debug("Trying to detect character set on %s" % parsed.netloc)
			try:
				charset = chardet.detect(raw_html)['encoding']
				self.logger.debug("Detected character set %s" % charset)
				html = raw_html.decode(charset)
			except:
				self.logger.warn("Detected character set was wrong")
				self.page_error(url, self.status['DECODE_ERROR'])
				return True

			# convert from unicode to bytes for classifier, replacing characters that aren't valid in ascii
		try:
				html_ascii = html.encode('ascii', 'xmlcharrefreplace')
		except:
				self.logger.warn("Error encoding for classifier")
				self.page_error(url, self.status['DECODE_ERROR'])
				return True

		try:
			self.logger.debug("Getting Classifier at %f" % time.clock())
			classifier = self.classifier_queue.get(block=True)
			self.logger.debug('Classifying')
			probability = classifier.classify(html_ascii)
		except focuser.WebsterClassifierError, e:
			self.logger.warn("Classifying page %s failed with error %s" % (url, e))
			self.page_error(url, self.status['CLASSIFY_ERROR'])
			return True
		finally:
			self.logger.debug("Putting Classifier at %f"  % time.clock())
			self.classifier_queue.put(classifier)

		if probability < self.cutoff:
			self.logger.debug("Probability %f not relevant" % probability)
			self.page_success(url, probability)
			return True

		unique_links = {}
		link_count = {}
		self.logger.debug('finding links')
		links = self.link_pattern.findall(html)
		for href in links:
			self.logger.debug("Found link %s" % href)
			# means start after href="
			target = urlparse.urlparse(href[6:-1])
			if self.exclude_pattern.search( target.path ):
				self.logger.debug("Link was to excluded filetype")
				continue
			if target.scheme != '' and target.scheme != 'http':
				self.logger.debug("Link was not http")
				continue
			# Add domain name to relative links
			if target.netloc == '':
				try:
					target = urlparse.urlparse( urlparse.urljoin(url, target.geturl()) )
				except:
					self.logger.warn('Error processing relative link')
					continue
			self.logger.debug('Storing link')
			# limit key to maximum length we can store in database
			unique_links[target.geturl()[:self.max_url_length]] = True
			count = link_count.setdefault(target.netloc, 0)
			link_count[target.netloc] = count + 1
			self.logger.debug("Incremented link count to %d" % link_count[target.netloc])
		self.page_success(url, probability, unique_links, link_count, html)
		return True

	def crawl_pages(self, count=100):
		start_time = time.time()
		for i in range(0, count):
			did_crawl = self.crawl_page()
			if did_crawl == False:
				return False
		ppm = count / ( (time.time() - start_time) / 60.0)
		self.logger.info("A thread crawled at %f pages per minute" % ppm)
		return True

# for quick testing with one crawler
if __name__ == '__main__':
	LOGGING_LEVELS = {'critical': logging.CRITICAL,
					'error': logging.ERROR,
					'warning': logging.WARNING,
					'info': logging.INFO,
					'debug': logging.DEBUG}

	parser = optparse.OptionParser()
	parser.add_option('-p', '--page-count', type='int', default=100, help='Number of pages to crawl')
	parser.add_option('-l', '--logging-level', help='Logging level')
	parser.add_option('-s', '--server', type='str', default='localhost', help='Address of WebsterDBServer')
	(options, args) = parser.parse_args()
	category = args[0]

	logging_level = LOGGING_LEVELS.get(options.logging_level, logging.NOTSET)
	logging.basicConfig(level=logging_level)
	logger = logging.getLogger('')

	classifier_queue = Queue.Queue(1)
	classifier_queue.put( focuser.WebsterClassifier(category) )
	w = Webster(Pyro.core.getProxyForURI("PYROLOC://%s:4242/webster" % options.server), classifier_queue)
	w.crawl_pages(options.page_count)
