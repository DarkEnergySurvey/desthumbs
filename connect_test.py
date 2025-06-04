#!/usr/bin/env python

import configparser
import oracledb
import os
import pandas as pd
import numpy
import collections

import sys
SOUT = sys.stdout


def load_db_config(config_file, profile):
    config = configparser.ConfigParser()
    config.read(config_file)

    section = dict(config[profile])
    section['dsn'] = f'{section["server"]}:{section["port"]}/{section["name"]}'
    return section


def check_columns(cols, req_cols):
    """ Test that all required columns are present"""
    for c in req_cols:
        if c not in cols:
            raise TypeError('column %s in file' % c)
    return


def get_archive_root(dbh, schema='prod', verb=False):

    QUERY_ARCHIVE_ROOT = {}

    name = {}
    name['prod'] = 'desar2home'

    QUERY_ARCHIVE_ROOT['prod'] = "select root from prod.ops_archive where name='%s'" % name['prod']
    if verb:
        SOUT.write("# Getting the archive root name for section: %s\n" % name[schema])
        SOUT.write("# Will execute the SQL query:\n********\n %s\n********\n" % QUERY_ARCHIVE_ROOT[schema])
    cur = dbh.cursor()
    cur.execute(QUERY_ARCHIVE_ROOT[schema])
    archive_root = cur.fetchone()[0]
    cur.close()
    return archive_root


def query2dict_of_columns(query, dbhandle, array=False):
    """
    Transforms the result of an SQL query and a Database handle object [dhandle]
    into a dictionary of list or numpy arrays if array=True
    """

    # Get the cursor from the DB handle
    cur = dbhandle.cursor()
    # Execute
    cur.execute(query)
    # Get them all at once
    list_of_tuples = cur.fetchall()
    # Get the description of the columns to make the dictionary
    desc = [d[0] for d in cur.description]

    querydic = collections.OrderedDict()  # We will populate this one
    cols = list(zip(*list_of_tuples))
    for k, val in enumerate(cols):
        key = desc[k]
        if array:
            if isinstance(val[0], str):
                querydic[key] = numpy.array(val, dtype=object)
            else:
                querydic[key] = numpy.array(val)
        else:
            querydic[key] = cols[k]
    return querydic


def find_tilename_radec(ra, dec, dbh, schema='prod'):

    if ra < 0:
        exit("ERROR: Please provide RA>0 and RA<360")

    QUERY_TILENAME_RADEC = {}
    # We match old table COADDTILE with COADDTILE_GEOM to match CROSSRA0 column
    QUERY_TILENAME_RADEC['des_admin'] = """
    select c.TILENAME from des_admin.COADDTILE c, prod.COADDTILE_GEOM g
           where c.TILENAME=g.TILENAME AND
                 (g.CROSSRA0='N' AND ({RA} BETWEEN URALL and URAUR) AND ({DEC} BETWEEN UDECLL and UDECUR)) OR
                 (g.CROSSRA0='Y' AND ({RA180} BETWEEN URALL-360 and URAUR-360) AND ({DEC} BETWEEN UDECLL and UDECUR))
    """
    # Special case for CROSSRA0
    QUERY_TILENAME_RADEC['prod'] = """
    select TILENAME from prod.COADDTILE_GEOM
           where (CROSSRA0='N' AND ({RA} BETWEEN RACMIN and RACMAX) AND ({DEC} BETWEEN DECCMIN and DECCMAX)) OR
                 (CROSSRA0='Y' AND ({RA180} BETWEEN RACMIN-360 and RACMAX) AND ({DEC} BETWEEN DECCMIN and DECCMAX))
    """

    # Case for DR/DR1
    QUERY_TILENAME_RADEC['dr1'] = """
    select TILENAME from des_admin.DR1_TILE_INFO
           where (CROSSRA0='N' AND ({RA} BETWEEN RACMIN and RACMAX) AND ({DEC} BETWEEN DECCMIN and DECCMAX)) OR
                 (CROSSRA0='Y' AND ({RA180} BETWEEN RACMIN-360 and RACMAX) AND ({DEC} BETWEEN DECCMIN and DECCMAX))
    """

    if ra > 180:
        ra180 = 360 - ra
    else:
        ra180 = ra
    query = QUERY_TILENAME_RADEC[schema].format(RA=ra, DEC=dec, RA180=ra180)
    tilenames_dict = query2dict_of_columns(query, dbh, array=False)

    if len(tilenames_dict) < 1:
        SOUT.write("# WARNING: No tile found at ra:%s, dec:%s\n" % (ra, dec))
        return False
    else:
        return tilenames_dict['TILENAME'][0]
    return


def find_tilenames_radec(ra, dec, dbh, schema='prod'):

    """
    Find the tilename for each ra,dec and bundle them as dictionaries per tilename
    """

    indices = {}
    tilenames = []
    tilenames_matched = []
    for k in range(len(ra)):

        tilename = find_tilename_radec(ra[k], dec[k], dbh, schema=schema)
        tilenames_matched.append(tilename)

        # Write out the results
        if not tilename:  # No tilename found
            # Here we could do something to store the failed (ra,dec) pairs
            continue

        # Store unique values and initialize list of indices grouped by tilename
        if tilename not in tilenames:
            indices[tilename] = []
            tilenames.append(tilename)

        indices[tilename].append(k)

    return tilenames, indices, tilenames_matched


def get_base_names(tilenames, ra, dec, prefix='DES'):
    names = []
    for k in range(len(ra)):
        if tilenames[k]:
            # name = get_thumbBaseName(ra[k], dec[k], prefix=prefix)
            name = f"{prefix}_{ra[k]}-{dec[k]}"
        else:
            name = False
        names.append(name)
    return names


if __name__ == "__main__":

    sout = sys.stdout

    # Read in CSV file with pandas
    inputList = 'bin/example_input_radec.csv'
    df = pd.read_csv(inputList)

    # Decide if we do search by RA,DEC or by COADD_ID
    searchbyID = False
    ra = df.RA.values
    dec = df.DEC.values
    nobj = len(ra)
    req_cols = ['RA', 'DEC']

    # Check columns for consistency
    check_columns(df.columns, req_cols)

    # Check the xsize and ysizes
    # xsize, ysize = check_xysize(df, args, nobj)

    db_section = 'db-desoper'
    schema = 'prod'

    config_file = os.path.join(os.environ['HOME'], 'dbconfig.ini')
    # Get the connection credentials and information
    creds = load_db_config(config_file, db_section)
    dbh = oracledb.connect(user=creds['user'],
                           password=creds['passwd'],
                           dsn=creds['dsn'])

    archive_root = get_archive_root(dbh, schema='prod', verb=True)
    print(archive_root)

    tilenames, indices, tilenames_matched = find_tilenames_radec(ra, dec, dbh, schema=schema)

    # Add them back to pandas dataframe and write a file
    df['TILENAME'] = tilenames_matched
    # Get the thumbname base names and the them the pandas dataframe too
    df['THUMBNAME'] = get_base_names(tilenames_matched, ra, dec, prefix="DES")
    matched_list = os.path.join(".", 'matched_'+os.path.basename(inputList))
    df.to_csv(matched_list, index=False)
    sout.write("# Wrote matched tilenames list to: %s\n" % matched_list)
