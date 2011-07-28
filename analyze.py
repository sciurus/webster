import MySQLdb
import optparse
import math
import matplotlib.pyplot as plt
from scipy import stats

parser = optparse.OptionParser()
parser.add_option('-m', '--mysql-address', type='str', default='localhost', help='Address of MySQL server')
(options, args) = parser.parse_args()
category = args[0]

status = {'NOT_VISITED': 0, 'IN_PROGRESS': 1, "VISITED": 2, "VISIT_ERROR": 3, "CLASSIFY_ERROR": 4, "DECODE_ERROR": 5}

conn = MySQLdb.connect(host=options.mysql_address, db=category, user='user', passwd='password', charset='utf8')
cursor = conn.cursor()

totals = {}

print "Analysis of %s" % category
print "-" * 80

# total # of pages
cursor.execute('SELECT count(id) FROM pages')
totals['all_pages'] = int(cursor.fetchone()[0])
print "Total pages are %d" % totals['all_pages']

# total # of missing
cursor.execute('SELECT count(status) FROM pages WHERE status = 3')
totals['missing_pages'] = int(cursor.fetchone()[0])
print "Missing pages are %d" % totals['missing_pages']

# total # of error
cursor.execute('SELECT count(status) FROM pages WHERE status = 4 OR status = 5')
totals['error_pages'] = int(cursor.fetchone()[0])
print "Error pages are %d" % totals['error_pages']

# total # of classified
cursor.execute('SELECT count(status) FROM pages WHERE status = 2')
totals['classified_pages'] = int(cursor.fetchone()[0])
print "Classified pages are %d" % totals['classified_pages']

# total # of relevant pages
cursor.execute('SELECT count(relevance) FROM pages WHERE relevance = 1.0')
totals['relevant_pages'] = int(cursor.fetchone()[0])
print "Relevant pages are %d" % totals['relevant_pages']

# total # of irrelevant pages
cursor.execute('SELECT count(relevance) FROM pages WHERE relevance <> 1.0')
totals['irrelevant_pages'] = int(cursor.fetchone()[0])
print "Irelevant pages are %d" % totals['irrelevant_pages']

# total # of sites
cursor.execute('SELECT COUNT(id) FROM sites')
totals['sites'] = int(cursor.fetchone()[0])
print "Total sites are %d" % totals['sites']

# total # links
cursor.execute('SELECT sum(all_inbound) FROM sites')
totals['all_inbound'] = int(cursor.fetchone()[0])
print "Total links are %d" % totals['all_inbound']

# total # external links
cursor.execute('SELECT sum(ext_inbound) FROM sites')
totals['ext_inbound'] = int(cursor.fetchone()[0])
print "Links across sites are %d" % totals['ext_inbound']

# each site and the number of links to it
cursor.execute('SELECT all_inbound FROM sites WHERE all_inbound IS NOT NULL ORDER BY all_inbound DESC LIMIT 50')
rows = cursor.fetchall()
site_counts = [row[0] for row in rows]
count =  float(site_counts[0])
print "Percentage of links to top site using all links is %f" % (count / totals['all_inbound'],)
sum=0
for i in range(0,10):
	sum += site_counts[i]
print "Percentage of links to top 10 using all links is %f" % (float(sum) / totals['all_inbound'],)
sum=0
for i in range(0,50):
	sum += site_counts[i]
print "Percentage of links to top 50 using all links is %f" % (float(sum) / totals['all_inbound'],)

# each site and the number of links to it, not including links from the same site
cursor.execute('SELECT ext_inbound from sites WHERE ext_inbound IS NOT NULL ORDER BY ext_inbound DESC LIMIT 50')
rows = cursor.fetchall()
site_counts = [row[0] for row in rows]
count = float(site_counts[0])
print "Percentage to top site using only external links is %f" % (count / totals['ext_inbound'],)
sum=0
for i in range(0,10):
	sum += site_counts[i]
print "Percentage to top 10 using only external links is %f" % (float(sum) / totals['ext_inbound'],)
sum=0
for i in range(0,50):
	sum += site_counts[i]
print "Percentage to top 50 using only external links is %f" % (float(sum) / totals['ext_inbound'],)

# should have stored added pages count to sites table in summarize.py to avoid join

cursor.execute('SELECT sites.hostname, sites.all_inbound, count(pages.id), sites.relevance FROM sites INNER JOIN pages ON sites.id = pages.site_id WHERE all_inbound IS NOT NULL GROUP BY sites.hostname ORDER BY all_inbound DESC LIMIT 10;')
print "Top 10 sites using all links, total pages, total links, and the percentage of their pages crawled that were relevant, are"
for i in range(0,10):
	(site, pages, links, relevance) = cursor.fetchone()
	print "%s & %s & %s & %s \\\\" % (site, pages, links, relevance)

cursor.execute('SELECT sites.hostname, sites.ext_inbound, count(pages.id), sites.relevance FROM sites INNER JOIN pages ON sites.id = pages.site_id WHERE ext_inbound IS NOT NULL GROUP BY sites.hostname ORDER BY ext_inbound DESC LIMIT 10;')
print "Top 10 sites using only external links, total pages, external links, and the percentage of their pages crawled that were relevant, are"
for i in range(0,10):
	(site, pages, links, relevance) = cursor.fetchone()
	print "%s & %s & %s & %s \\\\" % (site, pages, links, relevance)

# a number of links and the number of sites with that many links
cursor.execute('SELECT all_inbound, count(id) FROM sites WHERE all_inbound IS NOT NULL GROUP BY all_inbound ORDER BY all_inbound DESC')
rows = cursor.fetchall()
link_count = []
cumulative_sites = []
link_count.append(rows[0][0])
cumulative_sites.append(rows[0][1])
for i in range(1, cursor.rowcount):
	link_count.append(rows[i][0])
	cumulative_sites.append(rows[i][1] + cumulative_sites[i-1])

plt.loglog(cumulative_sites,link_count)

(slope,intercept,r,twotailedp,stderr)=stats.linregress([math.log(x) for x in link_count], [math.log(x) for x in cumulative_sites])
print('Regression testing power law fit using all links: R^2= %.3f p-value= %.3f, stderr= %.3f' % (r*r, twotailedp, stderr))

# a number of links and the number of sites with that many links
cursor.execute('SELECT ext_inbound, count(id) FROM sites WHERE ext_inbound IS NOT NULL GROUP BY ext_inbound ORDER BY ext_inbound DESC')
rows = cursor.fetchall()
link_count = []
cumulative_sites = []
link_count.append(rows[0][0])
cumulative_sites.append(rows[0][1])
for i in range(1, cursor.rowcount):
	link_count.append(rows[i][0])
	cumulative_sites.append(rows[i][1] + cumulative_sites[i-1])

plt.loglog(cumulative_sites,link_count)
plt.xlabel('Number of sites with at least Y inbound links')
plt.ylabel('Number of inbound links')
plt.savefig('%s_links.pdf' % category)

(slope,intercept,r,twotailedp,stderr)=stats.linregress([math.log(x) for x in link_count], [math.log(x) for x in cumulative_sites])
print('Regression testing power law fit using only external links: R^2= %.3f p-value= %.3f, stderr= %.3f' % (r*r, twotailedp, stderr))

print "\n"
