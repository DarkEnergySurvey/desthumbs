# -*- coding: utf-8 -*-
"""
Created on Fri Jun 19 14:31:14 2015

@author: rvg
"""

import despydb.desdbi as desdbi
import despyastro


#Function for reading columns
class getCol:
    matrix = []
    def __init__(self, file, delim=" "):
        with open(file, 'rU') as f:
            getCol.matrix =  [filter(None, l.split(delim)) for l in f]

    def __getitem__ (self, key):
        column = []
        for row in getCol.matrix:
            try:
                column.append(row[key])
            except IndexError:
                # pass
                column.append("")
        return column
#Reading in columns
desra = getCol('/Users/rvg/Documents/research/NCSA/des_testing_pos.txt')[0]
desdec = getCol('/Users/rvg/Documents/research/NCSA/des_testing_pos.txt')[1]

#Setup handle and cursor
dbh = desdbi.DesDbi(section='db-desoper')
cur = dbh.cursor()


#Gets tile name from RA and DEC
def ra_dec_to_tile(ra,dec):
        query1 = "select TILENAME from felipe.COADDTILE_NEW  where ((%s BETWEEN RACMIN and RACMAX) AND (%s BETWEEN DECCMIN and DECCMAX))" % (str(ra),str(dec))
        try:
            tiles = despyastro.query2rec(query1,dbh)    
        except:
            #raise Warning("No Tiles found")
            return False
        return tiles[0].TILENAME

#Gets archive root
archive_name = 'desardata'
def get_archive_root(archive_name,dbh):
        query = "select archive_root from archive_sites where location_name='%s'" % archive_name
        #print "# Getting the archive root name for section: %s" % archive_name
        #print "# Will execute the SQL query:\n********\n** %s\n********" % query
        cur = dbh.cursor()
        cur.execute(query)
        archive_root = cur.fetchone()[0]
        cur.close()
        return archive_root
        
#Gets file name from tile name, tag, and a band
#Use the name of the band (either 'r','g','Y','i', or 'z') as the dictionary key
#Use the <None> key on returned dictionary to obtain det file
#e.g. to print the r-band file:
        #tile_file = tile_to_file('Y1A1_COADD','DES0011+0209')
        #print tile_file['r']
#to print det file:
        #tile_file = tile_to_file('Y1A1_COADD','DES0011+0209')
        #print tile_file[None]
def tile_to_file(tag,tile_name):
        query2 = "select f.path, c.band from des_admin.COADD c, des_admin.filepath_desar f where c.RUN in (select RUN from des_admin.RUNTAG where TAG='%s') and c.TILENAME='%s' and f.ID=c.ID" % (tag,tile_name)
        root = get_archive_root(archive_name,dbh)
        try:
            f = despyastro.query2rec(query2,dbh)
        except:
            f_name2 = {'g':False,'z':False,'i':False,'Y':False,'r':False,None:False}
            return f_name2              
        f_name = {f[0].BAND:root+'/'+f[0].PATH,f[1].BAND:root+'/'+f[1].PATH,f[2].BAND:root+'/'+f[2].PATH,f[3].BAND:root+'/'+f[3].PATH,f[4].BAND:root+'/'+f[4].PATH,f[5].BAND:root+'/'+f[5].PATH}
        return f_name
#The two following functions take in a list for RA and DEC and output a dictionary where the keys are tile names. Inside those keys are lists of RAs/DECs that
#are found in that tile          
def ra_tile_dictionary(ra,dec):
        ra_dic = {}
    #storing all the tile names without duplicates
        tiles_all = []
        for i,items in enumerate(ra):
            tiles_all.append(ra_dec_to_tile(ra[i],dec[i]))
        tiles_uniq = list(set(tiles_all))
        
        #creating dictionary
        for j,item in enumerate(tiles_uniq):
            ra_tmp = []
            for k, item_1 in enumerate(ra):
                if tiles_uniq[j] == ra_dec_to_tile(ra[k],dec[k]):
                    ra_tmp.append(ra[k])
            ra_dic[tiles_uniq[j]] = ra_tmp
            
        return ra_dic

def dec_tile_dictionary(ra,dec):
        dec_dic = {}
    #storing all the tile names without duplicates
        tiles_all = []
        for i,items in enumerate(ra):
            tiles_all.append(ra_dec_to_tile(ra[i],dec[i]))
        tiles_uniq = list(set(tiles_all))
        
        #creating dictionary
        for j,item in enumerate(tiles_uniq):
            dec_tmp = []
            for k, item_1 in enumerate(ra):
                if tiles_uniq[j] == ra_dec_to_tile(ra[k],dec[k]):
                    dec[k] = dec[k].replace("\n", "") 
                    dec_tmp.append(dec[k])
            dec_dic[tiles_uniq[j]] = dec_tmp
            
        return dec_dic     
         
def file_name_dictionary(ra,dec,tag):
        bands = ['g','r','i','z','Y']
        file_dic = {}
    #storing all the tile names without duplicates
        tiles_all = []
        for i,items in enumerate(ra):
            tiles_all.append(ra_dec_to_tile(ra[i],dec[i]))
        tiles_uniq = list(set(tiles_all))
        
        #creating dictionary
        for j, item in enumerate(tiles_uniq):
           band_dic = {}
           for k,band in enumerate(bands):
               tile_file = []
               tile_file1 = tile_to_file(tag,tiles_uniq[j])
               tile_file.append(tile_file1[bands[k]])
               band_dic[bands[k]] = tile_file
           file_dic[tiles_uniq[j]] = band_dic
           
        return file_dic
           
#Testing
'''
for i, item in enumerate(desra):
    tilefile = tile_to_file('Y1A1_COADD',ra_dec_to_tile(desra[i],desdec[i]))
    print tilefile['i']
'''

#RAs = ra_tile_dictionary(desra,desdec)
#DECs = dec_tile_dictionary(desra,desdec)
#print RAs
#print DECs

file_name = file_name_dictionary(desra,desdec,'Y1A1_COADD')
#print file_name
print file_name['DES0008+0209']['Y']
