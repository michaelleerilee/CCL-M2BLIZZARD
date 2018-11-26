
import numpy as np
from netCDF4 import Dataset

from os      import listdir
from os.path import isfile, join

import traceback

from math import sqrt

###########################################################################

def matsuzawa_blizzard_mask(precsno,prectot,u10m,v10m,u2m,v2m,frland,frlandice,inf_fill=np.inf, verbose=False):

    # print 'precsno.shape: ',precsno.shape
    
    # mask_all_snow, iif (precsno > 0.  AND precsno = prectot AND (frland+frlandice) >= 0.5 , int8(1) , null)
    mask_all_snow   = np.where( (precsno >  0.) & (precsno == prectot) & ((frland+frlandice) >= 0.5 ) )
    mask_all_snow_i = np.where( (precsno <= 0.) | (precsno != prectot) | ((frland+frlandice) <  0.5 ) )
    
    # visibility = 707.95*(pow(s2m*(fact3+(fact2*fact1)),-0.773 ))*mask_all_snow
    # if mask_all_snow > 0
    
    # visibility       = np.inf
    visibility       = np.full(precsno.shape,inf_fill,np.float)
    extinction_coeff = 0.0
    mask_visib_800   = False
    mask_visib_1000  = False
    mask_visib_1200  = False
    
    # if mask_all_snow:
    if True:
        s10m  = np.zeros(precsno.shape,np.float)
        s2m   = np.zeros(precsno.shape,np.float)
        fact1 = np.zeros(precsno.shape,np.float)
        fact2 = np.zeros(precsno.shape,np.float)
        fact3 = np.zeros(precsno.shape,np.float)
        tmp   = np.zeros(precsno.shape,np.float)
        
        s10m  [:,:] = np.sqrt( (u10m*u10m) + (v10m * v10m) )
        s2m   [:,:] = np.sqrt( (u2m*u2m) + (v2m * v2m) )
        fact1 [:,:] = np.power( (2.0/0.15), (-0.35/(0.4*0.036*s10m) ) )
        fact2 [:,:] = 30.0 - (precsno*1000./1.2)
        fact3 [:,:] = precsno*1000./1.2
        
        try:
            tmp = s2m*(fact3+(fact2*fact1))
            idx  = np.where(tmp >  0)
            idx_ = np.where(tmp <= 0)
            visibility[idx]  = 707.95*(np.power(tmp[idx],-0.773 ))
            visibility[idx_] = inf_fill
            # extinction_coeff = 3.912/visibility,
            # mask_visib_800   = visibility <=  800
            # mask_visib_1000  = visibility <=  1000
            # mask_visib_1200  = visibility <=  1200
        except ZeroDivisionError :
            if verbose:
                print 'WARNING: visibility calculation ZeroDivisionError, setting to inf, inf_fill = ',inf_fill
                # print 'precsno,prectot,u10m,v10m,u2m,v2m,frland,frlandice:'\
                #    ,precsno,prectot,u10m,v10m,u2m,v2m,frland,frlandice
                # print 's10m,s2m,f1,f2,f3: ',s10m,s2m,fact1,fact2,fact3

    visibility[mask_all_snow_i] = inf_fill
    visibility[np.where(visibility > inf_fill)] = inf_fill
            
    # save to MERRA2_visib_with_land_landice
    # return mask_all_snow,visibility,mask_visib_800,mask_visib_1000,mask_visib_1200
    return visibility

###########################################################################

# M2T1NXFLX, precsno, prectot
# M2T1NXSLV, u10m, v10m, u2m, v2m
# M2C0NXASM, frland, frlandice
# <frland:float null, frlandice:float null>[z=0:0,1,0,y=0:360,91,0,x=0:575,144,0]

def m2_datafiles(\
                 M2BASE="/home/mrilee/nobackup/data/MERRA2/"
                 ,M2DATASET=""):
    M2DSPATH=M2BASE+M2DATASET+'/'
    # print 'M2DSPATH: ',M2DSPATH
    M2FILES=sorted([f for f in listdir(M2DSPATH) if isfile(join(M2DSPATH,f)) and '.nc4' in f ])
    # print 'M2FILES: ',M2FILES
    return (M2DSPATH,M2FILES)

# quit()

def date_from_name(name):
    return name.split('.')[2]
def compare_dates(name0,name1):
    return date_from_name(name0) == date_from_name(name1)

class input_array(object):
    def __init__(self,datafile_range=[0]):
        self.ndatafiles      = len(datafile_range)
        self.constfile_range = [0]
        self.visibility_i = []

        try:

            # M2C0NXASM_
            # There's only one of these files.
            name_base            = "M2C0NXASM"
            M2C0NXASM_arrays     = ['frland','frlandice']
            M2C0NXASM_base,M2C0NXASM_file_names \
                = m2_datafiles(M2DATASET=name_base)
            for i in self.constfile_range:
                file_name = M2C0NXASM_base+M2C0NXASM_file_names[i]
                M2C0NXASM_ds=Dataset(file_name)

            frland    = M2C0NXASM_ds['FRLAND']
            frlandice = M2C0NXASM_ds['FRLANDICE']
            
            # M2T1NXFLX_
            name_base            = "M2T1NXFLX"
            M2T1NXFLX_arrays     = ['PRECSNO','PRECTOT']
            M2T1NXFLX_base,M2T1NXFLX_file_names \
                = m2_datafiles(M2DATASET=name_base)

            # M2T1NXSLV_
            name_base            = "M2T1NXSLV"
            M2T1NXSLV_arrays     = 'U10M,V10M,U2M,V2M'.split(',')
            M2T1NXSLV_base,M2T1NXSLV_file_names \
                = m2_datafiles(M2DATASET=name_base)
            
            for i in datafile_range:
                # We need to find commensurate data from two different files. Flag error
                # if the files don't match.
                # print 'compare file names'
                if not compare_dates(M2T1NXFLX_file_names[i],M2T1NXSLV_file_names[i]):
                    print 'ERROR data file mismatch for iteration i = ',i
                    print 'mismatch: ',M2T1NXFLX_file_names[i],M2T1NXSLV_file_names[i]
                    print 'qutting...'
                    quit()
                    # print M2T1NXFLX_file_names[i],M2T1NXSLV_file_names[i]\
                        #     , compare_dates(M2T1NXFLX_file_names[i],M2T1NXSLV_file_names[i])
                    # print\
                        #     date_from_name(M2T1NXFLX_file_names[i])\
                        #     ,date_from_name(M2T1NXSLV_file_names[i])
                file_name = M2T1NXFLX_base+M2T1NXFLX_file_names[i]
                M2T1NXFLX_ds=Dataset(file_name)
                file_name = M2T1NXSLV_base+M2T1NXSLV_file_names[i]
                M2T1NXSLV_ds=Dataset(file_name)

                self.shape = M2T1NXFLX_ds['PRECSNO'].shape
                v_i = np.full(self.shape,np.nan,np.float)

                # print 'fl.sh  ',M2C0NXASM_ds ['FRLAND'    ].shape
                # print 'fli.sh ',M2C0NXASM_ds ['FRLANDICE' ].shape

                nt   = self.shape[0]
                nlat = self.shape[1]
                nlon = self.shape[2]

                for it in range(nt):
                    # print 'it: ',it
                    # mask_all_snow,visibility,mask_visib_800,mask_visib_1000,mask_visib_1200\
                    visibility\
                        = matsuzawa_blizzard_mask(
                            M2T1NXFLX_ds  ['PRECSNO'   ] [it,:,:]
                            ,M2T1NXFLX_ds ['PRECTOT'   ] [it,:,:]
                            ,M2T1NXSLV_ds ['U10M'      ] [it,:,:]
                            ,M2T1NXSLV_ds ['V10M'      ] [it,:,:]
                            ,M2T1NXSLV_ds ['U2M'       ] [it,:,:]
                            ,M2T1NXSLV_ds ['V2M'       ] [it,:,:]
                            ,M2C0NXASM_ds ['FRLAND'    ] [0, :,:]
                            ,M2C0NXASM_ds ['FRLANDICE' ] [0, :,:]
                        )
                    v_i[it,:,:] = 1.0/visibility
                    
                for i in range(nt):
                    self.visibility_i.append(v_i[i,:,:])
            
        except Exception as e:
            print 'ERROR: ',e.message
            print traceback.format_exc()


    def info(self):
        print 'len(visibility_i)  = ',len(self.visibility_i)
        print 'visibility_i.shape = ',self.visibility_i[0].shape
        print 'input data shape:    ',self.shape

    def dbg_vis_i(self,i_slice=0):
        print 'dbg_vis_i'
        np.set_printoptions(threshold=5000,linewidth=300,precision=0)
        print self.visibility_i[i_slice][0:30,0:30]
        np.set_printoptions(threshold=1000,linewidth=75,precision=8)

if __name__ == "__main__":

    if False:
        a = input_array()

    if True:
        a = input_array([0,1])
        
    a.info()
    a.dbg_vis_i()
    
    
    
