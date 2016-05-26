#!/usr/bin/env python
import logging as lg
import argparse
from impala_db import ImpalaDB
from impala.error import  DatabaseError

def print_year_month(part):
    #for year, month in part:
    (year, month) = part
    return ''.join([' partition( year=', year, ', month=', month, ') '])

def print_year_month_value(part):
    (year, month) = part
    return ''.join([year, '-', month])

def print_where_clause(part_field, part_string):
    return ''.join([' where ', part_field, '=\'', part_string, '\''])

def detect_schema_changes(old_schema, new_schema):
    off_set = len(old_schema) - len(new_schema)
    if off_set == 0:
        return (0, '')
    if off_set < -2:
        schema = new_schema
        update = 0
    else:
        schema = new_schema[off_set:]
        update = 1
    schema_string = ''
    for field_name, field_type, _ in schema:
        schema_string += ''.join([field_name, ' ', field_type, ','])
    schema_string = schema_string.rstrip(',')
    return update, schema_string



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="")
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--update', action='store_true',  help='update or create partitions' )
    group.add_argument('--merge', action='store_true', help='merge small files')

    parser.add_argument('--targetDB', default='postgres_refined',  help='target database')
    parser.add_argument('--targetTable', default='', help= 'target table')
    parser.add_argument('--sourceDB', default='postgres', help='source database')
    parser.add_argument('--sourceTable', default='', help='source table')
    parser.add_argument('--partitionField', default='updated_at', help='partition fields')

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
    try:
        if args.update:
            with ImpalaDB('impala-us.ds.avant.com', 21050) as impala_db :
                new_schema = impala_db.get_schema(args.sourceDB, args.sourceTable)
                old_schema = impala_db.get_schema(args.targetDB, args.targetTable)
                (update, schema_string) = detect_schema_changes(old_schema, new_schema)
                if update:
                    impala_db.update_partitions({'target_db':args.targetDB, 'target_table':args.targetTable, 'schema':schema_string})

                elif schema_string :
                    impala_db.create_partition_table({'target_db': args.targetDB, 'target_table': args.targetTable, 'schema': schema_string,
                         'partition_clause': 'partitioned by (year smallint, month smallint) ', 'table_format':'parquet'})

                part_string = ''.join(['substr(', args.partitionField, ',1,4), ', 'substr(', args.partitionField, ', 6,2) '])
                parts = impala_db.get_partitions(''.join(['select distinct ', part_string, ' from ', args.sourceDB, '.'
                                                             , args.sourceTable, ' limit 5']))
                for part in parts:
                    part_string = print_year_month(part)
                    year_month = print_year_month_value(part)
                    impala_db.update_partitions({'target_db':args.targetDB, 'source_db':args.sourceDB, 'target_table':args.targetTable,
                                             'source_table':args.sourceTable, 'part_clause':part_string, 'where_clause':
                                                 print_where_clause('substr(updated_at, 1, 7)', year_month)})

    except DatabaseError:
        lg.fatal('Impala DB operations failed!')
        exit(1)

