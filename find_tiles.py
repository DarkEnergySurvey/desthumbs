#!/usr/bin/env python

import pandas as pd
from despydb import desdbi
import despyastro

ALL_BANDS = "'g','r','i','z','Y'"


def get_archive_root(dbh,archive_name='desardata',verb=False):

    """ Gets the archive root for an archive_name -- usually /archive_data/Archive """
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

    QUERY_TILENAME_RADEC = """
    select TILENAME from felipe.COADDTILE_NEW
           where (({RA} BETWEEN RACMIN and RACMAX) AND ({DEC} BETWEEN DECCMIN and DECCMAX))
    """
    tilenames_dict = despyastro.query2dict_of_columns(QUERY_TILENAME_RADEC.format(RA=ra,DEC=dec),dbh,array=False)
    if len(tilenames_dict)<1:
        print "# WARNING: No tile found at ra:%s, dec:%s" % (ra,dec)
        return False
    else:
        return tilenames_dict['TILENAME'][0]
    return

def get_coaddfiles_tilename(tilename,tag,dbh,bands='all'):

    QUERY_COADDFILES_BANDS = """
    select distinct f.path, TILENAME, BAND from des_admin.COADD c, des_admin.filepath_desar f
             where
             c.BAND in ({BANDS}) and
             c.TILENAME='{TILENAME}' and
             f.ID=c.ID and
             c.RUN in (select RUN from des_admin.RUNTAG where TAG='{TAG}')"""

    if bands == 'all':
        sbands = ALL_BANDS
    else:
        sbands = "'" + "','".join(bands) + "'" # trick to format

    # Return a record array with the query
    return despyastro.query2rec(QUERY_COADDFILES_BANDS.format(TILENAME=tilename,TAG=tag,BANDS=sbands),dbh)

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

    # Defaults
    tag = 'Y1A1_COADD'
    bands = ['g','i','z']
    bands = 'all'

    # Get DB handle
    dbh = desdbi.DesDbi(section='db-desoper')

    # Get archive_root
    archive_root = get_archive_root(dbh,archive_name='desardata',verb=True)

    # Find all of the tilenames, for ra,dec array
    tilenames = []
    for k in range(len(ra)):
        tilename = find_tilename(ra[k],dec[k],dbh)
        if not tilename: # No tilename found
            # Here we could do something to store the failed (ra,dec) pairs
            continue
        # Store unique values
        if tilename not in tilenames:
            tilenames.append(tilename)

    filenames = {}
    for tilename in tilenames:
        filenames[tilename] = get_coaddfiles_tilename(tilename,tag,dbh,bands=bands)
        #print x.dtype.names
        print filenames[tilename].PATH



    print filenames['DES0005+0001'].PATH

    #print filenames['DES0005+0001'][ filenames['DES0005+0001']['BAND'] == 'z' ].PATH
    #print filenames['DES0005+0001'][ filenames['DES0005+0001']['BAND'] == 'r' ].PATH

    #for file in filenames['DES0005+0001']:
    #    print 

    #print filenames['DES0005+0001']['PATH']
        
