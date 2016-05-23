import warnings
import logging as lg
import argparse
from impala_db import ImpalaDB

def print_year_month(part):
    #for year, month in part:
    (year, month) = part
    return ''.join([' partition( year=', year, ', month=', month, ') '])
def print_year_month_value(part):
    (year, month) = part
    return ''.join([year, '-', month])

def print_where_clause(part_field, part_string):
    return ''.join([' where ', part_field, '=', part_string])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="")
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--update', action='store_true',  help='update or create partitions' )
    group.add_argument('--merge', action='store_true', help='merge small files')

    parser.add_argument('--targetDB', default='postgres',  help='target database')
    parser.add_argument('--targetTable', default='', help= 'target table')
    parser.add_argument('--sourceDB', default='postgres_refined', help='source database')
    parser.add_argument('--sourceTable', default='', help='source table')
    parser.add_argument('--partitionField', default='updated_at', help='partition fields')


    # set up logging
    lg.basicConfig(level=lg.DEBUG,
                   format='%(asctime)s %(name)-8s %(levelname)-5s %(message)s',
                   datefmt='%m-%d %H:%M',
                   filename='/tmp/aa_cli.log',
                   filemode='w')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = lg.StreamHandler()
    console.setLevel(lg.INFO)
    # set a format which is simpler for console use
    formatter = lg.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    lg.getLogger('').addHandler(console)

    # parse args
    args = parser.parse_args()
    try:
        if args.update:
            with ImpalaDB('10.73.50.24', 21050) as impala_db :
                schema = impala_db.get_schema(args.sourceDB, args.sourceTable)
                schema_string =''
                for field_name, field_type, _ in schema:
                    schema_string += ''.join([field_name, ' ', field_type, ','])
                schema_string = schema_string.rstrip(',')

                impala_db.create_partitions({'target_db':'postgres_refined', 'target_table':'jtest_python', 'schema':schema_string,
                                             'partition_clause': 'partitioned by (year smallint, month smallint) ', 'table_format':'parquet'})
                part_string = ''.join(['substr(', args.partitionField, ',1,4), ', 'substr(', args.partitionField, ', 6,2) '])
                parts = impala_db.get_partitions(''.join(['select ', part_string, ' from ', args.sourceDB, '.'
                                                             , args.sourceTable, ' limit 5']))
                for part in parts:
                    part_string = print_year_month(part)
                    year_month = print_year_month_value(part)
                    impala_db.update_partitions({'target_db':args.targetDB, 'source_db':args.sourceDB, 'target_table':args.targetTable,
                                             'source_table':args.sourceTable, 'part_clause':part_string, 'where_clause':
                                                 print_where_clause('substr(updated_at, 1, 7)', year_month)})
    except LookupError:
        pass

