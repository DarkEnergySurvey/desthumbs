#!/usr/bin/env python

"""
A set of simple proto-function to make postage stamps using fitsio
F. Menanteau, NCSA July 2015
"""

import fitsio
import os,sys
from despyastro import astrometry
from despyastro import wcsutil
import time
import numpy
import subprocess

# Naming template
FITS_OUTNAME = "{prefix}J{ra}{dec}_{filter}.{ext}"
TIFF_OUTNAME = "{prefix}J{ra}{dec}.{ext}"
STIFF_EXE = 'stiff'

# Definitions for the color filter sets we'd like to use, by priority
# depending on what BANDS will be combined
_CSET1 = ['i','r','g']
_CSET2 = ['z','r','g']
_CSET3 = ['z','i','g']
_CSET4 = ['z','i','r']
_CSETS = (_CSET1,_CSET2,_CSET3,_CSET4)

# Format time
def elapsed_time(t1,verb=False):
    import time
    t2    = time.time()
    stime = "%dm %2.2fs" % ( int( (t2-t1)/60.), (t2-t1) - 60*int((t2-t1)/60.))
    if verb:
        print >>sys.stderr,"Elapsed time: %s" % stime
    return stime

def get_coadd_hdu_extensions_byfilename(filename):
    """
    Return the HDU extension for coadds (old-school) based on the extension name.
    Check if dealing with .fz or .fits files
    """
    if os.path.basename(os.path.splitext(filename)[-1]) == '.fz':
        sci_hdu = 1
        wgt_hdu = 2
    elif os.path.basename(os.path.splitext(filename)[-1]) == '.fits' :
        sci_hdu = 0
        wgt_hdu = 1
    else:
        raise NameError("ERROR: No .fz or .fits files found")
    return sci_hdu, wgt_hdu

def update_wcs_matrix(header,x0,y0,naxis1,naxis2):

    """
    Update the wcs header object with the right CRPIX[1,2] CRVAL[1,2] for a given subsection
    """
    import copy
    # We need to make a deep copy/otherwise if fails
    h = copy.deepcopy(header)
    # Get the wcs object
    wcs = wcsutil.WCS(h)
    # Recompute CRVAL1/2 on the new center x0,y0
    CRVAL1,CRVAL2 = wcs.image2sky(x0,y0)
    # Asign CRPIX1/2 on the new image
    CRPIX1 = int(naxis1/2.0)
    CRPIX2 = int(naxis2/2.0)
    # Update the values
    h['CRVAL1'] = CRVAL1
    h['CRVAL2'] = CRVAL2
    h['CRPIX1'] = CRPIX1
    h['CRPIX2'] = CRPIX2
    return h

def check_inputs(ra,dec,xsize,ysize):

    """ Check and fix inputs for cutout"""
    # Make sure that RA,DEC are the same type
    if type(ra) != type(dec):
        raise TypeError('RA and DEC need to be the same type()')
    # Make sure that XSIZE, YSIZE are same type
    if type(xsize) != type(ysize):
        raise TypeError('XSIZE and YSIZE need to be the same type()')
    # Make them iterable and proper length
    if hasattr(ra,'__iter__') is False and hasattr(dec,'__iter__') is False:
        ra = [ra]
        dec = [dec]
    if hasattr(xsize,'__iter__') is False and hasattr(ysize,'__iter__') is False:
        xsize = [xsize]*len(ra)
        ysize = [ysize]*len(ra)
    # Make sure they are all of the same length
    if len(ra) != len(dec):
        raise TypeError('RA and DEC need to be the same length')
    if len(xsize) != len(ysize):
        raise TypeError('XSIZE and YSIZE need to be the same length')
    if (len(ra) != len(xsize)) or (len(ra) != len(ysize)):
        raise TypeError('RA, DEC and XSIZE and YSIZE need to be the same length')
    return ra,dec,xsize,ysize

def get_thumbFitsName(ra,dec,filter,prefix='DES',ext='fits'):
    """ Common function to set the Fits thumbnail name """
    ra  = astrometry.dec2deg(ra/15.,sep="",plussign=False)
    dec = astrometry.dec2deg(dec,   sep="",plussign=True)
    namekwargs = locals()
    outname = FITS_OUTNAME.format(**namekwargs)
    return outname

def get_thumbColorName(ra,dec,prefix='DES',ext='tif'):
    """ Common function to set the Fits thumbnail name """
    ra  = astrometry.dec2deg(ra/15.,sep="",plussign=False)
    dec = astrometry.dec2deg(dec,   sep="",plussign=True)
    namekwargs = locals()
    outname = TIFF_OUTNAME.format(**namekwargs)
    return outname

#def fitscutter_MP(x):
#    (filename, ra, dec, xsize, ysize,units,prefix) = x
#    return fitscutter(filename, ra, dec, xsize, ysize,units,prefix)

def fitscutter(filename, ra, dec, xsize=1.0, ysize=1.0, units='arcmin',prefix='DES'):

    """
    Makes cutouts around ra, dec for a give xsize and ysize
    ra,dec can be scalars or lists/arrays
    """
    # Check and fix inputs
    ra,dec,xsize,ysize = check_inputs(ra,dec,xsize,ysize)

    # Check for the units
    if units == 'arcsec':
        scale = 1
    elif units == 'arcmin':
        scale = 60
    elif units == 'degree':
        scale = 3600
    else:
        sys.exit("ERROR: must define units as arcses/arcmin/degree only")
 
    # Get the numbers for the file extensions
    (sci_hdu,wgt_hdu) = get_coadd_hdu_extensions_byfilename(filename)

    # Intitialize the FITS object
    ifits = fitsio.FITS(filename,'r')

    # Get the SCI and WGT headers
    h_sci = ifits[sci_hdu].read_header()
    h_wgt = ifits[wgt_hdu].read_header()

    # Get the pixel-scale of the input image
    pixelscale = astrometry.get_pixelscale(h_sci,units='arcsec')

    # Read in the WCS with wcsutil
    wcs = wcsutil.WCS(h_sci)

    ######################################
    # Loop over ra/dec and xsize,ysize
    for k in range(len(ra)):

        # Define the geometry of the thumbnail
        x0,y0 = wcs.sky2image(ra[k],dec[k])
        x0 = round(x0)
        y0 = round(y0)
        dx = int(0.5*xsize[k]*scale/pixelscale)
        dy = int(0.5*ysize[k]*scale/pixelscale)
        naxis1 = 2*dx#+1
        naxis2 = 2*dy#+1
        y1 = y0-dy
        y2 = y0+dy
        x1 = x0-dx
        x2 = x0+dx

        # Create a canvas
        im_section_sci = numpy.zeros((naxis1,naxis2))
        im_section_wft = numpy.zeros((naxis1,naxis2))
        
        # Read in the image section we want for SCI/WGT
        im_section_sci = ifits[sci_hdu][y1:y2,x1:x2]
        im_section_wgt = ifits[wgt_hdu][y1:y2,x1:x2]
        # Update the WCS in the headers and make a copy
        h_section_sci = update_wcs_matrix(h_sci,x0,y0,naxis1,naxis2)
        h_section_wgt = update_wcs_matrix(h_wgt,x0,y0,naxis1,naxis2)

        # Construct the name of the Thumbmail
        filter = h_sci['FILTER'].strip()
        outname = get_thumbFitsName(ra[k],dec[k],filter,prefix='DES')

        # Write out the file
        ofits = fitsio.FITS(outname,'rw',clobber=True)
        ofits.write(im_section_sci,header=h_section_sci)
        ofits.write(im_section_wgt,header=h_section_wgt)
        ofits.close()
        print >>sys.stderr,"# Wrote: %s" % outname
      
    return


def get_stiff_parameter_set(tiffname,**kwargs):
    """
    Set the Stiff default options and have the options to
    overwrite them with kwargs to this function.
    """
    stiff_parameters = {
        "OUTFILE_NAME"     : tiffname,
        "COMPRESSION_TYPE" : "JPEG",
        }
    stiff_parameters.update(kwargs)
    return stiff_parameters


def make_stiff_call(fitsfiles,tiffname,stiff_parameters={},list=False):

    """ Make the stiff call for a set of input FITS filenames"""

    pars = get_stiff_parameter_set(tiffname,**stiff_parameters)
    stiff_conf = os.path.join(os.environ['DESTHUMBS_DIR'],'etc','default.stiff')

    cmd_list = []
    cmd_list.append("%s" % STIFF_EXE)
    for fname in fitsfiles:
        cmd_list.append( "%s" % fname)
        
    cmd_list.append("-c %s" % stiff_conf)
    for param,value in pars.items():
        cmd_list.append("-%s %s" % (param,value))

    if list:
        cmd = cmd_list
    else:
        cmd = ' '.join(cmd_list)
    return cmd


def get_colorset(avail_bands,color_set=None):
    """
    Get the optimal color set for DES Survey for a set of available bands
    """
    # 1. Check if desired color_set matches the available bands """
    #if color_set:
    inset = list( set(color_set) & set(avail_bands))        
    if len(inset) == 3:
        return color_set

    # 2. Otherwise find the optimal one
    CSET = False
    for color_set in _CSETS:
        if CSET: break
        inset = list( set(color_set) & set(avail_bands))
        if len(inset) == 3:
            CSET = color_set
    # 3. If no match return False
    if not CSET:
        CSET=False 
    return CSET

def color_radec(ra,dec,avail_bands,prefix='DES',colorset=['i','r','g'], stiff_parameters={}):

    t0 = time.time()

    # Set the output tiff name
    tiffname = get_thumbColorName(ra,dec,prefix=prefix,ext='tif')

    # Get colorset or match with available bands
    CSET = get_colorset(avail_bands,colorset) 

    if CSET is False:
        print "# WARNING: Could not find a suitable filter set for color image for ra,dec: %s,%s" % (ra,dec)
        return 
    
    fitsfiles = []
    for BAND in CSET:
        fitsthumb = get_thumbFitsName(ra,dec,BAND,prefix='DES',ext='fits')
        fitsfiles.append( "%s" % fitsthumb)

    cmd = make_stiff_call(fitsfiles,tiffname,stiff_parameters={},list=False)
    status = subprocess.call(cmd,shell=True)#,stdout=log, stderr=log)
    if status > 0:
        print "***\nERROR while running Stiff***"
    else:
        print "# Total stiff time: %s" % desthumbs.elapsed_time(t0)

    return 

if __name__ == "__main__":

    # images taken from:
    # /archive_data/Archive/OPS/coadd/20141006000032_DES0002+0001/coadd/

    # Example of inputs:
    # ra,dec can be list or scalars
    ra  = [0.71925223,   0.61667249, 0.615752,    0.31218133]
    dec = [0.0081421517, 0.13929069, 0.070078051, 0.08508208]
    #xsize = [3]*len(ra)
    #ysize = [5]*len(ra)

    #xsize = [3,2]
    #ysize = [5,2]
    
    
    filename = 'DES0002+0001_g.fits.fz'
    t0 = time.time()
    cutout(filename, ra, dec, xsize=xsize, ysize=ysize, units='arcmin',prefix='DES')
    #cutout(filename, ra, dec, xsize=1, ysize=1, units='arcmin',prefix='DES')
    print "Done: %s" % elapsed_time(t0)

