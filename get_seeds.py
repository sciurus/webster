from web_search import google, msn, yahoo
from urllib import urlopen, unquote
import os

def search(query, limit):

	merged_results = {}

	results = open("google_%s" % query, 'w')
	for (name, url, desc) in google(query, limit):
		results.write(url + '\n')
		merged_results[url] = 1
	results.close()

	results = open("yahoo_%s" % query, 'w')
	for (name, url, desc) in yahoo(query, limit):
		# Get real url from yahoo redirect url
		url = unquote(url)
		real_start = url.rfind('http://')
		url = url[real_start:]
		results.write(url + '\n')
		merged_results[url] = 1
	results.close()

	results = open("msn_%s" % query, 'w')
	for (name, url, desc) in msn(query, limit):
		results.write(url + '\n')
		merged_results[url] = 1
	results.close()

	results = open("merged_%s" % query, 'w')
	for url in merged_results.keys():
		results.write(url + '\n')
	results.close()

	return merged_results.keys()


def save_results(query, urls):
	name = 0
	for url in urls:
		path = os.path.join(query, str(name))
		name += 1
		f = open(path, 'w')
		try:			
			u = urlopen(url)
			html = u.read()
		except IOError:
			print "Error fetching %s" % url
			f.close()
			os.remove(path)
		else:
			f.write(html)
			f.close()
			

if __name__ == "__main__":
	queries = ('global warming','gun control', 'gay marriage', 'free trade')
	limit = 200
	for query in queries:
		os.mkdir(query)
		urls = search(query, limit)
		save_results(query, urls)
