#!/usr/bin/env python

import pandas as pd
from despydb import desdbi
import despyastro

def connect_DB(db_section):
    dbh = desdbi.DesDbi(section=db_section)
    return dbh

def get_archive_root(dbh,archive_name='desardata',verb=False):

    """ Gets the archive root -- usually /archive_data/Archive """
    query = "select archive_root from archive_sites where location_name='%s'"  % archive_name
    if verb:
        print "# Getting the archive root name for section: %s" % archive_name
        print "# Will execute the SQL query:\n********\n** %s\n********" % query
    cur = dbh.cursor()
    cur.execute(query)
    archive_root = cur.fetchone()[0]
    cur.close()
    return archive_root

def find_tilename(ra,dec,dbh):


    QUERY_TILENAME = """
    select TILENAME from felipe.COADDTILE_NEW
           where (({RA} BETWEEN RACMIN and RACMAX) AND ({DEC} BETWEEN DECCMIN and DECCMAX))
    """
    cur = dbh.cursor()
    cur.execute(QUERY_TILENAME.format(RA=ra,DEC=dec))
    tilename, = cur.fetchone()
    return tilename

def get_coaddfiles_tilename(tilename,tag,dbh,bands='all'):

    archive_root = '/archive_data/Archive/'
    band = 'r'
    
    QUERY_COADDFILES_BAND = """
    select distinct f.path, TILENAME, BAND from des_admin.COADD c, des_admin.filepath_desar f
             where
             c.BAND='{BAND}' and
             c.TILENAME='{TILENAME}' and
             f.ID=c.ID and
             c.RUN in (select RUN from des_admin.RUNTAG where TAG='{TAG}')"""
    print QUERY_COADDFILES_BAND.format(TILENAME=tilename,TAG=tag,BAND=band)

    cur = dbh.cursor()
    cur.execute(QUERY_COADDFILES_BAND.format(TILENAME=tilename,TAG=tag,BAND=band))
    a = cur.fetchall()

    QUERY_COADDFILES_ALL = """
    select distinct f.path, TILENAME, BAND from des_admin.COADD c, des_admin.filepath_desar f
             where
             c.TILENAME='{TILENAME}' and
             f.ID=c.ID and
             c.RUN in (select RUN from des_admin.RUNTAG where TAG='{TAG}')"""
    #cur.execute(QUERY_COADDFILES_ALL.format(TILENAME=tilename,TAG=tag))
    #b = cur.fetchall()
    #print b

    return a

if __name__ == "__main__":

    # Todo:
    # - Add logic for more than one tile, get the closest
    # - Add logic to loop over RA,DEC
    # - Group by TILENAME the resulss
    # - use despyastro.query2rec() to get the filenames
    # - Add query to get archive_root
    # - Move query template to single location (?)
    
    df      = pd.read_csv('test.csv')
    opt_cols= ['RA','DEC','XSIZE','YSIZE','ID','COLOR']
    
    print 'Columns = ', df.columns

    # Test for columns
    for c in opt_cols:
        if c  in df.columns :
            print 'column %s in file' % c
        else:
            print 'column %s not in file' % c

    # this keep the pandas data frame
    ra  = df.RA
    dec = df.DEC
    # this only keeps values
    ra = df.RA.values #if you only want the values

    tag = 'Y1A1_COADD'

    # Get DB handle
    dbh = connect_DB(db_section='db-desoper')

    # Get archive_root
    archive_root = get_archive_root(dbh,archive_name='desardata',verb=True)

    tilenames = []
    for k in range(len(ra)):
        tilename = find_tilename(ra[k],dec[k],dbh)
        if tilename not in tilenames:
            tilenames.append(tilename)
        

    for tilename in tilenames:
        a = get_coaddfiles_tilename(tilename,tag,dbh,bands='all')
        print a
        
