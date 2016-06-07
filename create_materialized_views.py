#!/usr/bin/env python
import logging as lg
import argparse
from impala_db import ImpalaDB
from impala.error import  DatabaseError
import string


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="")

    parser.add_argument('--targetDB', default='postgres', help='target database')
    parser.add_argument('--targetTable', default='', help='target table')
    parser.add_argument('--viewTable', default='', help='materialized view name')

    lg.basicConfig(level=lg.DEBUG,
                   format='%(asctime)s %(name)-8s %(levelname)-5s %(message)s',
                   datefmt='%m-%d %H:%M',
                   filename='/tmp/nightly_ingestion.log',
                   filemode='w')
    console = lg.StreamHandler()
    console.setLevel(lg.INFO)
    formatter = lg.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    lg.getLogger('').addHandler(console)

    args = parser.parse_args()
    with ImpalaDB('impala-us.ds.avant.com', 21050) as impala_db:
        schema = impala_db.get_schema(args.targetDB, 'postgres_avant_basic_us_' + args.targetTable + '_1')
        id = ('id', 'int', '')
        select_str =''
        if id not in schema:
            raise ValueError('Table does not have id field')
        schema.pop(0)
        for field_name, field_type, _ in schema:
            select_str += 'c.' + field_name + ','
        select_str = select_str.rstrip(',')

        ctas = string.Template("""
        create table $table_name stored as parquet as
        select a.id, $select
        from
        (select id, max(updated_at) as updated_at from $old_table_name group by id ) a
        left anti join postgres.postgres_avant_basic_us_deleted_entries_1 b on a.id = b.id and b.table_name='$old_table_name'
        inner join $old_table_name c on a.id = c.id and a.updated_at = c.updated_at

        """)
        sql = ctas.safe_substitute(table_name='postgres_refined' + '.' + args.viewTable, old_table_name=args.targetDB + '.postgres_avant_basic_us_'+ args.targetTable + '_1', select=select_str )
        impala_db.update_partitions(sql)
