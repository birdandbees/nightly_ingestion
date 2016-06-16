#!/usr/bin/env python
import logging as lg
import argparse
from impala_db import ImpalaDB
from impala.error import  DatabaseError

def print_year_month(part):
    #for year, month in part:
    try:
        (year, month) = part
        if not ( int(year) > 2000 and int(year) <= 2050) or not ( int(month) > 0 and int(month) < 13):
            return None
        return ''.join([' partition( partition_year=', year, ', partition_month=', month, ') '])
    except:
        return None


def print_where_clause(part, partition_field):
    (year, month) = part
    return ''.join([' where date_part(', partition_field, ',\'year\')=', year, ' and date_part(', partition_field, ',\'month\')=', month])

def discover_schema(schema):
    part = 0
    part_year = ('partition_year', 'smallint', '')
    part_month = ('partition_month', 'smallint', '')
    updated_at =('updated_at', 'timestamp', '')
    partition_field = 'updated_at'

    if part_year in schema:
        schema.remove(part_year)
        part += 1
    if part_month in schema:
        schema.remove(part_month)
        part += 1

    if updated_at not in schema and part != 0 :
        partition_field = 'created_at'


    schema_string=[]
    for field_name, field_type, _ in schema:
        if field_type == 'string':
            schema_string.append(field_name)
        else:
            schema_string.append('cast(`' + field_name + '` as ' + field_type + ')')
    return ','.join(schema), part, partition_field



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="")
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--update', action='store_true',  help='update or create partitions' )
    group.add_argument('--merge', action='store_true', help='merge small files')

    parser.add_argument('--targetDB', default='postgres_refined',  help='target database')
    parser.add_argument('--targetTable', default='', help= 'target table')
    parser.add_argument('--sourceDB', default='postgres', help='source database')
    parser.add_argument('--sourceTable', default='', help='source table')


    # TODO move logging config to a config file
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
            with ImpalaDB('kbor-shall-be-castrated.com', 21050, lg) as impala_db :
                schema = impala_db.get_schema(args.targetDB, args.targetTable)
                (schema_string, part, partition_field) = discover_schema(schema)

                if  part == 0:
                    # just insert
                    impala_db.create_nonpartition_table(
                        {'target_db': args.targetDB, 'target_table': args.targetTable, 'schema': schema_string,
                         'table_format': 'parquet', 'source_db': args.sourceDB, 'source_table': args.sourceTable} )
                else:
                    # get partition list
                    part_string = ''.join(
                        ['date_part(', partition_field, '\'year\')', 'date_part(', partition_field, '\'month\')'])
                    parts = impala_db.get_partitions(
                        ''.join(['select distinct ', part_string, ' from ', args.sourceDB, '.'
                                    , args.sourceTable]))

                    for part in parts:
                        part_string = print_year_month(part)
                        if part_string is not None:
                            impala_db.update_partitions(
                                {'target_db': args.targetDB, 'source_db': args.sourceDB,
                                 'target_table': args.targetTable,
                                 'source_table': args.sourceTable, 'part_clause': part_string, 'where_clause':
                                     print_where_clause(part, partition_field)})


    except DatabaseError:
        lg.fatal('Impala DB operations failed!')
        exit(1)

