-- To create a new table on dessci for Y6A2 called felipe.Y6A2_COADD_FILEPATH:
create table Y6A2_COADD_FILEPATH as
 select c.FILENAME, c.TILENAME, c.BAND, f.PATH, f.COMPRESSION
  from des_admin.Y6A2_COADD c, des_admin.Y6A2_FILE_ARCHIVE_INFO f
   where c.FILETYPE='coadd' and
         f.FILENAME=c.FILENAME;

-- to create a new table on desoper
create table Y6A2_COADD_FILEPATH as
  select c.FILENAME, c.TILENAME, c.BAND, f.PATH, f.COMPRESSION
  from prod.COADD c, prod.PROCTAG, prod.FILE_ARCHIVE_INFO f
         where prod.PROCTAG.TAG='Y6A2_COADD' and
           c.FILETYPE='coadd' and
           c.PFW_ATTEMPT_ID=PROCTAG.PFW_ATTEMPT_ID and
           f.FILENAME=c.FILENAME;
