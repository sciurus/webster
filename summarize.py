import MySQLdb
import optparse

def set_inbound():
	cursor.execute('ALTER TABLE sites ADD COLUMN all_inbound INTEGER')
	cursor.execute('ALTER TABLE sites ADD COLUMN ext_inbound INTEGER')
	for id in all_sites:
		cursor.execute('SELECT sum(count) FROM links WHERE to_id = %s', (id,))
		all_inbound, = cursor.fetchone()
		cursor.execute('SELECT sum(count) from links WHERE to_id = %s AND from_id <> to_id;', (id,))
		ext_inbound, = cursor.fetchone()
		cursor.execute('UPDATE sites SET all_inbound = %s WHERE id = %s', (all_inbound, id))
		cursor.execute('UPDATE sites SET ext_inbound = %s WHERE id = %s', (ext_inbound, id))

def set_relevance():
	cursor.execute('ALTER TABLE sites ADD COLUMN relevance FLOAT')
	cursor.execute('ALTER TABLE sites ADD INDEX (relevance)')
	for id in all_sites:
		cursor.execute('SELECT COUNT(relevance) FROM pages WHERE relevance = 1.0 AND site_id = %s', (id,))
		relevant, = cursor.fetchone()
		cursor.execute('SELECT COUNT(id) FROM pages WHERE site_id = %s', (id,))
		total, = cursor.fetchone()
		relevance = float(relevant) / float(total)
		relevance = round(relevance, 2)
		cursor.execute('UPDATE sites SET relevance = %s where id = %s', (relevance, id))

parser = optparse.OptionParser()
parser.add_option('-m', '--mysql-address', type='str', default='localhost', help='Address of MySQL server')
(options, args) = parser.parse_args()
category = args[0]

conn = MySQLdb.connect(host=options.mysql_address, db=category, user='user', passwd='password', charset='utf8')
cursor = conn.cursor()

cursor.execute('SELECT id FROM sites')
all_sites = [row[0] for row in cursor.fetchall()]

set_relevance()
set_inbound()
