##desthumbs

A python module to make FITS files cutouts/thumbnails

Description
-----------

Set of libraries and scripts to create thumbnails/cutouts from DES co-added
images for a give release TAG and color-composed RGB images.

Features
--------
- It reads the inputs postions (RA,DEC) in decimals form a CSV file, with optional XSIZE,YSIZE in arcminutes.
- Can be run single or multi-threaded
- Uses fitsio to open/write files
- Used stiff to make color images
- Can choose bands (--bands option) to cut from.

Examples
--------

To get the thumbnails for the postions in the file: inputfile_radec.csv in multi-process mode

```
   makeDESthumbs inputfile_radec.csv --xsize 1.5 --ysize 1.5 --tag Y1A1_COADD --MP
```

to get the thumbnails for a list COADD_OBJECTS_ID

```
   makeDESthumbs inputfile_id.csv --xsize 1.5 --ysize 1.5 --tag Y1A1_COADD --coaddtable Y1A1_COADD_OBJECTS --MP
```
