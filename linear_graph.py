import MySQLdb
import optparse
import math
import matplotlib.pyplot as plt
from scipy import stats

parser = optparse.OptionParser()
parser.add_option('-m', '--mysql-address', type='str', default='localhost', help='Address of MySQL server')
(options, args) = parser.parse_args()
category = args[0]
count = int(args[1])

conn = MySQLdb.connect(host=options.mysql_address, db=category, user='user', passwd='password', charset='utf8')
cursor = conn.cursor()

# a number of links and the number of sites with that many links
cursor.execute("SELECT all_inbound, count(id) FROM sites WHERE all_inbound IS NOT NULL GROUP BY all_inbound ORDER BY all_inbound DESC LIMIT %d" % (count))
rows = cursor.fetchall()
link_count = []
cumulative_sites = []
link_count.append(rows[0][0])
cumulative_sites.append(rows[0][1])
for i in range(1, cursor.rowcount):
	link_count.append(rows[i][0])
	cumulative_sites.append(rows[i][1] + cumulative_sites[i-1])

plt.plot(cumulative_sites,link_count)

# a number of links and the number of sites with that many links
cursor.execute("SELECT ext_inbound, count(id) FROM sites WHERE ext_inbound IS NOT NULL GROUP BY ext_inbound ORDER BY ext_inbound DESC LIMIT %d" % (count))
rows = cursor.fetchall()
link_count = []
cumulative_sites = []
link_count.append(rows[0][0])
cumulative_sites.append(rows[0][1])
for i in range(1, cursor.rowcount):
	link_count.append(rows[i][0])
	cumulative_sites.append(rows[i][1] + cumulative_sites[i-1])

plt.plot(cumulative_sites,link_count)
plt.xlabel('Number of sites with at least Y inbound links')
plt.ylabel('Number of inbound links')
plt.savefig('%s_linear_%d.pdf' % (category, count))

print "\n"
