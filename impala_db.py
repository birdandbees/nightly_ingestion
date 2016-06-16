import warnings
import logging
from impala.dbapi import connect
from impala.error import HiveServer2Error

class ImpalaDB:
  def __init__(self, host, port, logger=None):
    self.db = connect(host=host, port=port)
    self.cursor = self.db.cursor()
    self.logger = logger or logging.getLogger(__name__)

  def __enter__(self):
      return self

  def __exit__(self, exc_type, exc_val, exc_tb):
      self.cursor.close()
      self.db.close()

  def execute(self, cmd):
      self.logger.debug(cmd)
      self.cursor.execute(cmd)
      return self.cursor.fetchall()

  def update(self, cmd):
      self.logger.debug(cmd)
      self.cursor.execute(cmd)

  def get_schema(self, database, table):
      try:
        schema = self.execute("describe " + database + "." + table)
        return schema
      except HiveServer2Error:
        return ()


  def get_partitions(self, cmd):
      self.logger.debug(cmd)
      return self.execute(cmd)

  def update_partitions(self, *args, **kwargs):
      args = args[0]
      sql = ''.join(["insert into ", args['target_db'], ".", args['target_table'],  args['part_clause'],
                      " select ", args['schema'], " from ", args['source_db'], ".", args['source_table'], args['where_clause'] ])
      self.logger.debug(sql)
      self.update(sql)

  def update_nonpartition(self, *args, **kwargs):
      args = args[0]
      sql = ''.join(["insert overwrite into", args['target_db'], ".", args['target_table'],
                     " select ", args['schema'], " from ", args['source_db'], ".", args['source_table'] ] )
      self.logger.debug(sql)
      self.update(sql)


