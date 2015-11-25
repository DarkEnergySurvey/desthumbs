#!/usr/bin/env python

import os,sys
import pandas
import numpy
import time
import multiprocessing as mp

import desthumbs 
from despydb import desdbi
from despymisc.miscutils import elapsed_time 
import cx_Oracle

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
     parser.add_argument("--coaddtable", type=str, action="store", default=None ,
                         help="COADD table name to query if COADDS_ID are provided instead of RA,DEC in the input csv file")
     parser.add_argument("--bands", type=str, action='store', nargs = '+', default='all',
                         help="Bands used for images. Can either be 'all' (uses all bands, and is the default), or a list of individual bands")
     parser.add_argument("--prefix", type=str, action='store', default='DES',
                         help="Prefix for thumbnail filenames [default='DES']")
     parser.add_argument("--colorset", type=str, action='store', nargs = '+', default=['i','r','g'],
                         help="Color Set to use for creation of color image [default=i r g]")
     parser.add_argument("--MP", action='store_true', default=False,
                         help="Run in multiple core [default=False]")
     parser.add_argument("--verb", action='store_true', default=False,
                         help="Turn on verbose mode [default=False]")
     parser.add_argument("--outdir", type=str, action='store', default=os.getcwd(),
                         help="Output directory location [default='./']")
     parser.add_argument("--user", type=str, action='store',
                         help="Username")
     parser.add_argument("--password", type=str, action='store', help="password")
     args = parser.parse_args()

     print "# Will run:"
     print "# %s " % parser.prog
     for key in vars(args):
         print "# \t--%-10s\t%s" % (key,vars(args)[key])

     return args


def check_xysize(args,nobj):

    # Check if  xsize,ysize are set from command-line or read from csv file
    if args.xsize: xsize = numpy.array([args.xsize]*nobj)
    else:
        try: xsize = df.XSIZE.values
        except: xsize = numpy.array([XSIZE_default]*nobj)
        
    if args.ysize: ysize = numpy.array([args.ysize]*nobj)
    else:
        try: ysize = df.YSIZE.values
        except: ysize = numpy.array([YSIZE_default]*nobj)

    return xsize,ysize

def check_columns(cols,req_cols):

    """ Test that all required columns are present"""
    for c in req_cols:
         if c not in cols :
              raise TypeError('column %s in file' % c)
    return

def run(args):
     
    # Read in CSV file with pandas
    df = pandas.read_csv(args.inputList)
    
    # Decide if we do search by RA,DEC or by COADD_ID
    if 'COADD_OBJECTS_ID' in df.columns:
         searchbyID = True
         if not args.coaddtable:
              raise Exception("ERROR: Need to provide --coaddtable <tablename> when using COADD_OBJECTS_ID")
         coadd_id =  dec = df.COADD_OBJECTS_ID.values
         nobj = len(coadd_id)
         req_cols = ['COADD_OBJECTS_ID']
    else:
         searchbyID = False
         ra  = df.RA.values #if you only want the values otherwise use df.RA
         dec = df.DEC.values
         nobj = len(ra)
         req_cols = ['RA','DEC']
    
    # Check columns for consistency
    check_columns(df.columns,req_cols)
    
    # Check the xsize and ysizes
    xsize,ysize = check_xysize(args,nobj)

    # Get DB handle
    if args.user and args.password:
         print "# Both deinfe"
         section = "db-desoper"
         host = 'leovip148.ncsa.uiuc.edu'
         port = '1521'
         name = 'desoper'
         kwargs = {'host': host, 'port': port, 'service_name': name}
         dsn = cx_Oracle.makedsn(**kwargs)
         dbh = cx_Oracle.connect(args.user, args.password, dsn=dsn)
    else:
         dbh = desdbi.DesDbi(section='db-desoper')

    # Get archive_root
    archive_root = desthumbs.get_archive_root(dbh,archive_name='desardata',verb=False)

    # Find all of the tilenames, indices grouped per tile
    if args.verb: print "# Finding tilename for each input position"
    if searchbyID:
         tilenames,ra,dec,indices = desthumbs.find_tilenames_id(coadd_id,args.coaddtable,dbh)
    else:
         tilenames,indices = desthumbs.find_tilenames_radec(ra,dec,dbh)

    # Make sure that all found tilenames *are* in the tag (aka data exists for them)
    #tilenames_intag = desthumbs.get_tilenames_in_tag(dbh,args.tag)


    # Make sure that outdir exists
    if not os.path.exists(args.outdir):
         if args.verb: print "# Creating: %s" % args.outdir
         os.makedirs(args.outdir)

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
        filenames = desthumbs.get_coaddfiles_tilename_bytag(tilename,dbh,args.tag,bands=args.bands)
        if filenames is False:
             print "# Skipping: %s -- not in TAG:%s " % (tilename,args.tag)
             continue
        indx      = indices[tilename]
        avail_bands = filenames.BAND

        # 2. Loop over all of the filename -- We could use multi-processing
        p={}
        for f in filenames.PATH:
            filename = os.path.join(archive_root,f)
            ar = (filename, ra[indx], dec[indx])
            kw = {'xsize':xsize[indx], 'ysize':ysize[indx],
                  'units':'arcmin', 'prefix':args.prefix, 'outdir':args.outdir,'verb':args.verb}
            if args.verb: print "# Cutting: %s" % filename
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
                                  outdir=args.outdir,
                                  verb=args.verb,
                                  stiff_parameters={'NTHREADS':NP})

        if args.verb: print "# Time %s: %s" % (tilename,elapsed_time(t1))

    print "\n*** Grand Total time:%s ***\n" % elapsed_time(t0)
    return 
