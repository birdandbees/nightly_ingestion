from impala.dbapi import connect
conn = connect(host='impala-us.ds.avant.com', port=21050)
cursor = conn.cursor()
cursor.execute('describe postgres_refined.jtest_merge')
print cursor.description  # prints the result set's schema
results = cursor.fetchall()
print results
