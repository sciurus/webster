from web_search import dmoz
from urllib import urlopen
from time import sleep
from socket import setdefaulttimeout
import codecs
import os

if __name__ == "__main__":
	base = 'negative'
	limit = 20
	name = 0
	# dont wait long for slow servers
	setdefaulttimeout(15)
	# read file as utf-8
	queries = codecs.open('negative_queries', 'r', 'utf-8')
	log = codecs.open('negative_log', 'a', 'utf-8')
	results = open("negative_urls", 'a')
	for query in queries:
		log.write("Searching for %s\n" % query)
		# write immediately so easier to see progress
		log.flush()
		try:
			for (title, url, desc) in dmoz(query, limit):
				results.write(url + '\n')
				results.flush()
				path = os.path.join(base, str(name))
				name += 1
				f = open(path, 'w')
				try:			
					u = urlopen(url)
					html = u.read()
				except:
					log.write( "\tError fetching %d from %s\n" % (name, url) )
					f.close()
					os.remove(path)
				else:
					f.write(html)
					f.close()
		except:
			log.write("Error searching for %s\n" % query)
		sleep(1)
		# ensure we wait at least 1 second between queries
	queries.close()			
	results.close()
