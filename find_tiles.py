#!/usr/bin/env python

import os,sys
import pandas
from despydb import desdbi
import despyastro
import desthumbs
import numpy
import time

XSIZE_default = 1.0
YSIZE_default = 1.0

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

def find_tilenames(ra,dec,dhn):

    """
    Find the tilename for each ra,dec and bundle them as dictionaries per tilename
    """

    indices = {}
    tilenames = []
    for k in range(len(ra)):

        tilename = find_tilename(ra[k],dec[k],dbh)
        if not tilename: # No tilename found
            # Here we could do something to store the failed (ra,dec) pairs
            continue
        # Store unique values and initialize list of indices grouped by tilename
        if tilename not in tilenames:
            indices[tilename]  = []
            tilenames.append(tilename)

        indices[tilename].append(k)

    return tilenames, indices 

def get_coaddfiles_tilename(tilename,dbh,tag,bands='all'):

    QUERY_COADDFILES_BANDS = """
    select distinct f.path, TILENAME, BAND from des_admin.COADD c, des_admin.filepath_desar f
             where
             c.BAND in ({BANDS}) and
             c.TILENAME='{TILENAME}' and
             f.ID=c.ID and
             c.RUN in (select RUN from des_admin.RUNTAG where TAG='{TAG}')"""

    QUERY_COADDFILES_ALL = """
    select distinct f.path, TILENAME, BAND from des_admin.COADD c, des_admin.filepath_desar f
             where
             c.BAND IS NOT NULL and
             c.TILENAME='{TILENAME}' and
             f.ID=c.ID and
             c.RUN in (select RUN from des_admin.RUNTAG where TAG='{TAG}')"""

    if bands == 'all':
        rec = despyastro.query2rec(QUERY_COADDFILES_ALL.format(TILENAME=tilename,TAG=tag),dbh)
    else:
        sbands = "'" + "','".join(bands) + "'" # trick to format
        rec = despyastro.query2rec(QUERY_COADDFILES_BANDS.format(TILENAME=tilename,TAG=tag,BANDS=sbands),dbh)
        
    # Return a record array with the query
    return rec 

def cmdline():
     import argparse
     parser = argparse.ArgumentParser(description="Retrieves FITS images within DES given the file and other parameters")
     
     # The positional arguments
     parser.add_argument("inputList", help="Input CSV file with positions (RA,DEC) and optional (XSIZE,YSIZE) in arcmins")
     
     # The optional arguments for image retrieval
     parser.add_argument("--xsize", type=float, action="store", default=None,
                         help="Length of x-side in arcmins of image [default = 1]")
     parser.add_argument("--ysize", type=float, action="store", default=None,
                         help="Length of y-side of in arcmins image [default = 1]")
     parser.add_argument("--tag", type=str, action="store", default = 'Y1A1_COADD',
                         help="Tag used for retrieving files [default=Y1A1_COADD]")
     parser.add_argument("--bands", type=str, action='store', nargs = '+', default='all',
                         help="Bands used for images. Can either be 'all' (uses all bands, and is the default), or a list of individual bands")
     parser.add_argument("--prefix", type=str, action='store', default='DES',
                         help="Prefix for thumbnail filenames [default='DES']")
     parser.add_argument("--colorset", type=str, action='store', nargs = '+', default=['i','r','g'],
                         help="Color Set to use for creation of color image [default=i r g]")
     args = parser.parse_args()

     print "# Will run:"
     print "# %s " % parser.prog
     for key in vars(args):
         print "# \t--%-10s\t%s" % (key,vars(args)[key])

     return args

if __name__ == "__main__":

    # Get the command-line arguments
    args = cmdline()

    # Read in CSV file with pandas
    df = pandas.read_csv(args.inputList)

    ## Test that all required columns are present
    req_cols = ['RA','DEC']
    for c in req_cols:
        if c not in df.columns :
            raise TypeError('column %s in file' % c)

    # this only keeps values
    ra  = df.RA.values #if you only want the values otherwise use df.RA
    dec = df.DEC.values

    # Check if  xsize,ysize are set from command-line or read from csv file
    if args.xsize: xsize = numpy.array([args.xsize]*len(ra))
    else:
        try: xsize = df.XSIZE.values
        except: xsize = numpy.array([XSIZE_default]*len(ra))
        
    if args.ysize: ysize = numpy.array([args.ysize]*len(ra))
    else:
        try: ysize = df.YSIZE.values
        except: ysize = numpy.array([YSIZE_default]*len(ra))

    # Get DB handle
    dbh = desdbi.DesDbi(section='db-desoper')

    # Get archive_root
    archive_root = get_archive_root(dbh,archive_name='desardata',verb=False)

    # Find all of the tilenames, indices grouped per tile
    tilenames,indices = find_tilenames(ra,dec,dbh)

    # Loop over all of the tilenames
    for tilename in tilenames:
        print "# Doing: %s" % tilename

        # 1. Get all of the filenames for a given tilename
        filenames = get_coaddfiles_tilename(tilename,dbh,args.tag,bands=args.bands)
        indx      = indices[tilename]

        # 2. Loop over all of the filename -- We could use multi-processing
        for f in filenames.PATH:
            filename = os.path.join(archive_root,f)
            print "# Cutting: %s" % filename
            desthumbs.fitscutter(filename, ra[indx], dec[indx], xsize=xsize[indx], ysize=ysize[indx], units='arcmin',prefix=args.prefix)

        # 3. Create color images using stiff for each ra,dec and loop over (ra,dec)
        avail_bands = filenames.BAND
        NTHREADS = len(avail_bands)
        for k in range(len(ra[indx])):
            desthumbs.color_radec(ra[indx][k],dec[indx][k],avail_bands,
                                  prefix=args.prefix,
                                  colorset=args.colorset,
                                  stiff_parameters={'NTHREADS':NTHREADS})
           
