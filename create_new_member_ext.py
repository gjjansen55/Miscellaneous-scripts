#!/usr/bin/python3

"""
create_new_member_ext.py

From a FirstLogic format file, generate the DDL to
create an external table based on COPE.NEW_MEMBER.

NOTE THAT the conventions are those used in the external files
that we have set up for the membership update system.
NOTE FURTHER that the stored procedure called on the database side
expects no schema prefixed to the table name and no quotation marks
around the table name.
"""

import argparse
import configparser
import logging
import os.path
import sys

import cx_Oracle

_REQUIRED_INPUTS = ['dbconfig_path', 'fmt_path', 'affiliate_acronym']

_TEMPLATE = """CREATE TABLE %(table_name)s
  (%(columns_with_lengths)s
  )
  ORGANIZATION EXTERNAL
   (TYPE ORACLE_LOADER
    DEFAULT DIRECTORY %(directory_name)s
    ACCESS PARAMETERS
    ( records delimited by newline
      badfile %(directory_name)s:'%(lc_table_name)s.bad'
      logfile %(directory_name)s:'%(lc_table_name)s.log'
      fields
      (%(columns_with_offsets)s)
    )
    LOCATION
    ( '%(file_name)s'
    )
  )
  REJECT LIMIT UNLIMITED
"""


def log_and_quit(message):
    """ log an error, bail out. """

    logging.error(message)
    sys.exit()


def get_cursor(username, password, dbname):
    """ used now in gen_ddl since we check the byte/char on columns. """

    conn = cx_Oracle.Connection(username, password, dbname)

    return conn.cursor()


def gen_ddl(all_args, cur):
    """ all_args is a dictionary, having entries for
          fmt_path: the pathname of a legitimate FirstLogic format file.
          table_name: the name of the table to create
          directory_name: the _Oracle_ directory
          file_name: the name of the file for the external table.
    """

    def get_char_semantics_columns(cur):
        """ Some of the columns in NEW_MEMBER have character semantics for their
        length. Get return a set of them. """

        cur.execute("""SELECT column_name
        FROM all_tab_columns
        WHERE owner = 'COPE'
          AND table_name = 'NEW_MEMBER'
          AND data_type = 'VARCHAR2'
          AND char_used = 'C'""")

        return set([row[0] for row in cur.fetchall()])

    def get_all_columns(cur):
        """ Let's make sure that the format file and the table match up. """

        cur.execute("""SELECT column_name
        FROM all_tab_columns
        WHERE owner = 'COPE'
          AND table_name = 'NEW_MEMBER'
          AND column_name NOT LIKE '%_VOTING_KEY'""")

        return set([row[0] for row in cur.fetchall()])

    def collect_columns(fmt_path, char_columns, db_columns):
        """ dig the information out of the format file. """

        all_columns = set([])
        retval = {}
        offset = 0
        with_lengths = []
        with_offsets = []
        try:
            with open(fmt_path, 'r') as ifh:
                for line in ifh:
                    parts = line.split(',')
                    name, length = parts[0].upper(), int(parts[1])
                    if name == 'EOR':
                        break
                    all_columns.add(name.upper())
                    if name in char_columns:
                        char_used = 'CHAR'
                    else:
                        char_used = 'BYTE'
                    with_lengths.append(
                        f'{name} VARCHAR2({length} {char_used})')
                    first = offset + 1
                    last = offset + length
                    with_offsets.append(f'"{name}"\tPOSITION({first}:{last})')
                    offset += length
                discrepancies = db_columns ^ all_columns
                if discrepancies:
                    print('Column discrepancies found for',
                          ', '.join(list(discrepancies)))
                    exit()
        except Exception as exc:
            log_and_quit(exc)

        retval['columns_with_lengths'] = ',\n    '.join(with_lengths)
        retval['columns_with_offsets'] = ',\n       '.join(with_offsets)

        return retval


    char_columns = get_char_semantics_columns(cur)
    db_columns = get_all_columns(cur)
    acronym_upper = all_args['affiliate_acronym'].upper()
    acronym_lower = all_args['affiliate_acronym'].lower()
    values = collect_columns(all_args['fmt_path'], char_columns, db_columns)
    values['table_name'] = all_args['table_name']
    values['lc_table_name'] = values['table_name'].lower()
    values['directory_name'] = all_args['directory_name']
    values['file_name'] = f'{acronym_lower}_good.txt'

    
    return _TEMPLATE % values


def create_table(cur, table_name, ddl):
    """
    Given a database connection, a table name, and the ddl,
    create a temporary table.
    """


    cur.callproc('code.ddl_utilities.create_or_replace_external_table',
                 [table_name, ddl])


def expand_args(args_dict):
    """ the database connection details and the database directory information
    are to be contained in the configuration file. We extract them here.
    we also create the table_name entry since it will be used in the
    function create_table.
    """

    affiliate_acronym = args_dict['affiliate_acronym']
    args_dict['table_name'] = f'NEW_MEMBER_{affiliate_acronym}_EXT'
    dbconfig_path = args_dict['dbconfig_path']
    if not os.path.isfile(dbconfig_path):
        log_and_quit('No such file: %s' % dbconfig_path)
    cfg = configparser.ConfigParser()
    cfg.read(dbconfig_path)
    if not cfg.has_section('database'):
        log_and_quit('No database found')
    for item in ['username', 'password', 'dbname', 'directory_name']:
        if cfg.has_option('database', item):
            args_dict[item] = cfg.get('database', item)
        else:
            log_and_quit('Missing parameter %s ' % item)

    return args_dict


parser = argparse.ArgumentParser('Generate DDL for an external table')
for item in _REQUIRED_INPUTS:
    parser.add_argument(item, type=str)
parser.add_argument('--show-only', dest='show_only',
                    action='store_const',
                    const=True, default=False,
                    help='display the DDL and execute without running')

args = expand_args(vars(parser.parse_args()))
cur = get_cursor(args['username'], args['password'], args['dbname'])
ddl = gen_ddl(args, cur)
if args['show_only']:
    print(ddl)
else:
    create_table(cur, args['table_name'], ddl)
