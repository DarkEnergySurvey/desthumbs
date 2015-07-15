
"""
Set of functions to find Tilenames and Filenames in the archive (old
Schema) for a set of positions (ra,dec)
F. Menanteau, NCSA July 2015
"""

import despyastro

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

    # For now we will use the old (and wrong) set of corner
    # definitions to be consistent with the Old Schema
    QUERY_TILENAME_RADEC_OLDSCHEMA = """
    select TILENAME from COADDTILE
           where (({RA} BETWEEN URALL and URAUR) AND ({DEC} BETWEEN UDECLL and UDECUR))
    """

    # In the future we want to use this one
    QUERY_TILENAME_RADEC = """
    select TILENAME from felipe.COADDTILE_NEW
           where (({RA} BETWEEN RACMIN and RACMAX) AND ({DEC} BETWEEN DECCMIN and DECCMAX))
    """
    #tilenames_dict = despyastro.query2dict_of_columns(QUERY_TILENAME_RADEC.format(RA=ra,DEC=dec),dbh,array=False)
    tilenames_dict = despyastro.query2dict_of_columns(QUERY_TILENAME_RADEC_OLDSCHEMA.format(RA=ra,DEC=dec),dbh,array=False)
    
    if len(tilenames_dict)<1:
        print "# WARNING: No tile found at ra:%s, dec:%s" % (ra,dec)
        return False
    else:
        return tilenames_dict['TILENAME'][0]
    return

def find_tilenames(ra,dec,dbh):

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

