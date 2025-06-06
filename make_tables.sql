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


select i.FILENAME, i.BAND, i.FILETYPE, i.EXPTIME, i.NITE, i.EXPNUM, e.DATE_OBS from Y6A2_IMAGE i, Y6A2_EXPOSURE e
  where ((i.CROSSRA0='N' AND (0.29782658 BETWEEN i.RACMIN and i.RACMAX) AND (0.029086056 BETWEEN i.DECCMIN and i.DECCMAX)) OR
        (i.CROSSRA0='Y' AND (0.29782658 BETWEEN i.RACMIN-360 and i.RACMAX) AND (0.029086056 BETWEEN i.DECCMIN and i.DECCMAX)))
        and i.EXPNUM=e.EXPNUM
        and i.BAND='r'
        and i.FILETYPE='red_immask';
