#!/usr/bin/env python

import os,sys
import pandas
import desthumbs 
from despydb import desdbi
import numpy
import time
from despymisc.miscutils import elapsed_time 
import multiprocessing as mp

XSIZE_default = 1.0
YSIZE_default = 1.0

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
     parser.add_argument("--MP", action='store_true', default=False,
                         help="Run in multiple core [default=False]")
     args = parser.parse_args()

     print "# Will run:"
     print "# %s " % parser.prog
     for key in vars(args):
         print "# \t--%-10s\t%s" % (key,vars(args)[key])

     return args

if __name__ == "__main__":

    # TODO:
    # - Cotrol logginh
    # - Move function to libraries:
    #   desthumbs.py --> desthumblib.py
    #   find_tiles.py --> tilefinder.py
    #   find_tiles.py --> makeDESthumbs

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
    archive_root = desthumbs.get_archive_root(dbh,archive_name='desardata',verb=False)

    # Find all of the tilenames, indices grouped per tile
    tilenames,indices = desthumbs.find_tilenames(ra,dec,dbh)

    # Loop over all of the tilenames
    t0 = time.time()
    Ntile = 0
    for tilename in tilenames:

        t1 = time.time()
        Ntile = Ntile+1
        print "# ----------------------------------------------------"
        print "# Doing: %s [%s/%s]" % (tilename,Ntile,len(tilenames))
        print "# -----------------------------------------------------"

        # 1. Get all of the filenames for a given tilename
        filenames = desthumbs.get_coaddfiles_tilename(tilename,dbh,args.tag,bands=args.bands)
        indx      = indices[tilename]
        avail_bands = filenames.BAND

        # 2. Loop over all of the filename -- We could use multi-processing
        p={}
        for f in filenames.PATH:
            filename = os.path.join(archive_root,f)
            ar = (filename, ra[indx], dec[indx])
            kw = {'xsize':xsize[indx], 'ysize':ysize[indx], 'units':'arcmin', 'prefix':args.prefix}
            print "# Cutting: %s" % filename
            if args.MP:
                NP = len(avail_bands)
                p[filename] = mp.Process(target=desthumbs.fitscutter, args=ar, kwargs=kw)
                p[filename].start()
            else:
                NP = 1
                desthumbs.fitscutter(*ar,**kw)

        # Make sure all process are closed before proceeding
        if args.MP:
            for filename in p.keys(): p[filename].join()

        # 3. Create color images using stiff for each ra,dec and loop over (ra,dec)
        for k in range(len(ra[indx])):
            desthumbs.color_radec(ra[indx][k],dec[indx][k],avail_bands,
                                  prefix=args.prefix,
                                  colorset=args.colorset,
                                  stiff_parameters={'NTHREADS':NP})
        print "Tile:%s time:%s" % (tilename,elapsed_time(t1))

    print "GrandTotal time:%s" % elapsed_time(t0)
