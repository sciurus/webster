import MySQLdb
import optparse

def drop_missing(keepers):
	cursor.execute('SELECT id FROM sites')
	all_sites = set( [row[0] for row in cursor.fetchall()] )
	goners = list( all_sites - keepers )
	for id in goners:
		cursor.execute('DELETE FROM pages WHERE site_id = %s' % (id,))
		cursor.execute('DELETE FROM links WHERE to_id = %s' % (id,))
		cursor.execute('DELETE FROM sites WHERE id = %s' % (id,))
	cursor.execute('OPTIMIZE TABLE pages')
	cursor.execute('OPTIMIZE TABLE links')
	cursor.execute('OPTIMIZE TABLE sites')
	return len(goners)

parser = optparse.OptionParser()
parser.add_option('-m', '--mysql-address', type='str', default='localhost', help='Address of MySQL server')
(options, args) = parser.parse_args()
category = args[0]

conn = MySQLdb.connect(host=options.mysql_address, db=category, user='user', passwd='password', charset='utf8')
cursor = conn.cursor()

# drop all results from sites that were not visited
cursor.execute('SELECT distinct site_id FROM pages')
visited_sites = set( [row[0] for row in cursor.fetchall()] )
pruned_count = drop_missing(visited_sites)
print "Pruned %d sites that were never visited in %s" % (pruned_count, category)

# drop all results from sites that had no relevant pages
cursor.execute('SELECT distinct site_id FROM pages WHERE relevance = 1.0')
relevant_sites = set( [row[0] for row in cursor.fetchall()] )
pruned_count = drop_missing(relevant_sites)
print "Pruned %d sites that had no relevant pages in %s" % (pruned_count, category)
