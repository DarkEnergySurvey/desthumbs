-- To create a new table on dessci for Y6A2 called felipe.Y6A2_COADD_FILEPATH:
-- DROP TABLE felipe.Y6A2_COADD_FILEPATH
create table Y6A2_COADD_FILEPATH as
 select c.FILENAME, c.TILENAME, c.BAND, c.FILETYPE, f.PATH, f.COMPRESSION
  from des_admin.Y6A2_COADD c, des_admin.Y6A2_FILE_ARCHIVE_INFO f
   where f.FILENAME=c.FILENAME;
--         and c.FILETYPE='coadd';

-- to create a new table on desoper
create table Y6A2_COADD_FILEPATH as
  select c.FILENAME, c.TILENAME, c.BAND, f.PATH, f.COMPRESSION
  from prod.COADD c, prod.PROCTAG, prod.FILE_ARCHIVE_INFO f
         where prod.PROCTAG.TAG='Y6A2_COADD' and
--           c.FILETYPE='coadd' and
           c.PFW_ATTEMPT_ID=PROCTAG.PFW_ATTEMPT_ID and des_admin.Y6A2
           f.FILENAME=c.FILENAME;


--- build table with info for finalcut files
create table Y6A2_FINALCUT_FILEPATH as
select i.FILENAME, f.PATH, i.BAND, i.EXPTIME, i.AIRMASS, i.FWHM, i.NITE, i.EXPNUM, i.CCDNUM, e.DATE_OBS, e.MJD_OBS,
       i.CROSSRA0, i.RACMIN, i.RACMAX, i.DECCMIN, i.DECCMAX,
       i.RAC1, i.RAC2, i.RAC3, i.RAC4, i.DECC1, i.DECC2, i.DECC3, i.DECC4
 from Y6A2_IMAGE i, Y6A2_EXPOSURE e, Y6A2_FILE_ARCHIVE_INFO f
 where  i.EXPNUM=e.EXPNUM
        and i.FILENAME=f.FILENAME
        and i.FILETYPE='red_immask';

-- example of search near ra, dec
select i.FILENAME, i.BAND, i.FILETYPE, i.EXPTIME, i.NITE, i.EXPNUM, e.DATE_OBS, e.MJD_OBS from Y6A2_IMAGE i, Y6A2_EXPOSURE e
  where ((i.CROSSRA0='N' AND (0.29782658 BETWEEN i.RACMIN and i.RACMAX) AND (0.029086056 BETWEEN i.DECCMIN and i.DECCMAX)) OR
        (i.CROSSRA0='Y' AND (0.29782658 BETWEEN i.RACMIN-360 and i.RACMAX) AND (0.029086056 BETWEEN i.DECCMIN and i.DECCMAX)))
        and i.EXPNUM=e.EXPNUM
        and i.BAND='r'
        and i.FILETYPE='red_immask';

-- example of search near ra, dec
select i.FILENAME, i.BAND, i.EXPTIME, i.NITE, i.EXPNUM, i.DATE_OBS, i.MJD_OBS from felipe.Y6A2_FINALCUT_FILEPATH i
  where ((i.CROSSRA0='N' AND (0.29782658 BETWEEN i.RACMIN and i.RACMAX) AND (0.029086056 BETWEEN i.DECCMIN and i.DECCMAX)) OR
  (i.CROSSRA0='Y' AND (0.29782658 BETWEEN i.RACMIN-360 and i.RACMAX) AND (0.029086056 BETWEEN i.DECCMIN and i.DECCMAX)))
  and i.BAND='r';
