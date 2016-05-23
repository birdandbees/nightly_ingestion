import warnings
import logging
from impala.dbapi import connect

class ImpalaDB:
  def __init__(self, host, port):
    self.db = connect(host=host, port=port)
    self.cursor = self.db.cursor()

  def __enter__(self):
      return self

  def __exit__(self, exc_type, exc_val, exc_tb):
      self.cursor.close()
      self.db.close()

  def execute(self, cmd):
      self.cursor.execute(cmd)
      return self.cursor.fetchall()

  def update(self, cmd):
      self.cursor.execute(cmd)

  def get_schema(self, database, table):
      return self.execute("describe " + database + "." + table)


  def create_partitions(self, *args, **kwargs):
      args = args[0]
      sql = ''.join(["create table if not exists ", args['target_db'], ".", args['target_table'], "(", args['schema'], ") ", args['partition_clause'],
                   " stored as ", args['table_format'] ])
      self.update(sql)

  def get_partitions(self, cmd):
      return self.execute(cmd)

  def update_partitions(self, *args, **kwargs):
      args = args[0]
      sql = ''.join(["insert into ", args['target_db'], ".", args['target_table'],  args['part_clause'],
                      " select * from ", args['source_db'], ".", args['source_table'], args['where_clause'] ])
      self.update(sql)

