#!/usr/bin/env python

# -*- coding: utf-8 -*-
"""
Created on Wed Jul  8 14:25:24 2015

@author: rvg
"""

# The main fuction to fill up all of the options
def cmdline():
     import argparse
     parser = argparse.ArgumentParser(description="Retrieves FITS images within DES given the file and other parameters")
     
     # The positional arguments
     parser.add_argument("inputPositions", help="Input Positions")
     
     #The optional arguments for image retrieval
     parser.add_argument("--xsize", type = float, action="store", default = 1.0,
                       help="Length of x-side in arcmins of image [default = 1]")
     parser.add_argument("--ysize", type = float, action="store", default = 1.0,
                       help="Length of y-side of in arcmins image [default = 1]")
     parser.add_argument("--tag", type = str, action="store", default = 'Y1A1_COADD',
                       help="Tag used for retrieving files [default=Y1A1_COADD]")
     parser.add_argument("--band", type = str, action='store', nargs = '+', default='all',
                       help="Bands used for images. Can either be 'all' (uses all bands, and is the default), or a list of individual bands")
     
     
     args = parser.parse_args()


     print "# Will run:"
     print "# %s " % parser.prog
     for key in vars(args):
         print "# \t--%-10s\t%s" % (key,vars(args)[key])

     return args

if __name__ == "__main__":
  args = cmdline()
  print args