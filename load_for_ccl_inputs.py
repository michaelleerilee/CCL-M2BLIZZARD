
# print 'load_for_ccl_inputs: starting...'
# print 'load_for_ccl_inputs: importing ccl'
from ccl_marker_stack import ccl_marker_stack
# print 'load_for_ccl_inputs: ccl imported'

import gzip
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
from math import sqrt

# print 'load_for_ccl_inputs: imports done'

# def matsuzawa_blizzard_mask(precsno,prectot,u10m,v10m,u2m,v2m,frland,frlandice,inf_fill=9998):

def matsuzawa_blizzard_mask(precsno,prectot,u10m,v10m,u2m,v2m,frland,frlandice,inf_fill=np.inf, verbose=False):
    # mask_all_snow, iif (precsno > 0.  AND precsno = prectot AND (frland+frlandice) >= 0.5 , int8(1) , null)
    mask_all_snow = ( precsno > 0. and precsno == prectot and (frland+frlandice) >= 0.5 )
    
    # visibility = 707.95*(pow(s2m*(fact3+(fact2*fact1)),-0.773 ))*mask_all_snow
    # if mask_all_snow > 0
    
    # visibility       = np.inf
    visibility       = inf_fill
    extinction_coeff = 0.0
    mask_visib_800   = False
    mask_visib_1000  = False
    mask_visib_1200  = False
    
    if mask_all_snow:

        s10m = sqrt( (u10m*u10m) + (v10m * v10m) )
        s2m  = sqrt( (u2m*u2m) + (v2m * v2m) )
        fact1 = pow ( (2.0/0.15), (-0.35/(0.4*0.036*s10m) ) )
        fact2 = 30.0 - (precsno*1000./1.2)
        fact3 = precsno*1000./1.2
        
        try:
            visibility = 707.95*(pow(s2m*(fact3+(fact2*fact1)),-0.773 ))
            extinction_coeff = 3.912/visibility,
            mask_visib_800   = visibility <=  800
            mask_visib_1000  = visibility <=  1000
            mask_visib_1200  = visibility <=  1200
        except ZeroDivisionError :
            if verbose:
                print 'WARNING: visibility calculation ZeroDivisionError, setting to inf, inf_fill = ',inf_fill
                print 'precsno,prectot,u10m,v10m,u2m,v2m,frland,frlandice:'\
                    ,precsno,prectot,u10m,v10m,u2m,v2m,frland,frlandice
                print 's10m,s2m,f1,f2,f3: ',s10m,s2m,fact1,fact2,fact3

    if visibility > inf_fill:
        visibility =  inf_fill
            
    # save to MERRA2_visib_with_land_landice
    return mask_all_snow,visibility,mask_visib_800,mask_visib_1000,mask_visib_1200


###########################################################################

# data schema t,z,y,x,precsno,prectot,u10m,v10m,u2m,v2m,frland,frlandice

class ccl_input_element(object):
    schema_attributes='t,z,y,x,precsno,prectot,u10m,v10m,u2m,v2m,frland,frlandice'.split(',')
    schema_types=[int,int,int,int,float,float,float,float,float,float,float,float]
    def __init__(self,line):
        self.vals    = {}
        vals         = line.split(',')
        for i in range(len(self.schema_attributes)):
            self.vals[self.schema_attributes[i]] = self.schema_types[i](vals[i])

        mask_all_snow,visibility,mask_visib_800,mask_visib_1000,mask_visib_1200\
            = matsuzawa_blizzard_mask(
                 self.vals['precsno']
                ,self.vals['prectot']
                ,self.vals['u10m']
                ,self.vals['v10m']
                ,self.vals['u2m']
                ,self.vals['v2m']
                ,self.vals['frland']
                ,self.vals['frlandice']
            )
        self.vals['visibility']      = visibility
        self.vals['mask_visib_1000'] = mask_visib_1000
        self.vals['visibility_i']    = 1.0/visibility

    def info(self):
        print 'ccl_input_element'
        print 'len:   ',len(self.schema_attributes)
        print 'attrs: ',self.schema_attributes
        print '--'

def load_for_ccl_inputs(file_name,attrs=[('visibility_i',np.nan,np.float)]):

    # print 'load_for_ccl_inputs args'
    # print 'file_name: ',file_name
    # print 'attrs:     ',attrs

    # return None

    returnVars = {}
    for v in attrs:
        # print 'attrs.v: ',v
        returnVars[v[0]] = {}

    with gzip.open(file_name,'r') as ccl_input:
        print 'loading: ',file_name
        ccl_input.readline() # Get past the headers.
        t_current = -1
        for line in ccl_input:
            
            ccl_input_elt = ccl_input_element(line)
            # ccl_inmput_elt.info()

            new_maps = False
            if ccl_input_elt.vals['t'] != t_current:
                if t_current < 0:
                    # First item
                    t_current = ccl_input_elt.vals['t']
                    new_maps = True
                elif ccl_input_elt.vals['t'] != t_current:
                    t_current = ccl_input_elt.vals['t']
                    if str(t_current) not in returnVars[attrs[0][0]].keys():
                        new_maps = True

            if new_maps:
                # print 'adding new map at t = ',t_current
                nlat = 361
                nlon = 576
                for v in attrs:
                    returnVars[v[0]][str(t_current)] = np.full((nlat,nlon),v[1],v[2])

            for v in attrs:
                returnVars[v[0]][str(t_current)]\
                    [ccl_input_elt.vals['y'],ccl_input_elt.vals['x']] = ccl_input_elt.vals[v[0]]


    keys = map(str,sorted(map(int,returnVars[attrs[0][0]].keys())))
    retArrays = {}
    for v in attrs:
        retArrays[v[0]] = [returnVars[v[0]][k] for k in keys]

    if len(retArrays) == 1:
        return retArrays.values()[0]
    
    return retArrays

    # print 'len(precsno):           ',len(precsno)
    # keys        = map(str,sorted(map(int,precsno.keys())))
    # precsno_arr    = [precsno[k]    for k in keys]
    # visibility_arr = [visibility[k] for k in keys]
    # print 'len (precsno_arr):      ',len (precsno_arr)
    # print 'type(precsno_arr):      ',type(precsno_arr)
    # print 'type(precsno_arr[0]):   ',type(precsno_arr[0])
    # print 'shape(precsno_arr[0]):  ',precsno_arr[0].shape
    # return (precsno_arr,visibility_arr)


if __name__ == "__main__":

    ###########################################################################
    # Files
    
    base='/home/mrilee/nobackup/tmp/others/'

    if False:
        fnames     = ['ccl-inputs-globe-122736+23.csv.gz'
                      ,'ccl-inputs-globe-122760+23.csv.gz']

    if True:
        fnames     = ['ccl-inputs-globe-122736+23.csv.gz'
                      ,'ccl-inputs-globe-122760+23.csv.gz'
                      ,'ccl-inputs-globe-122784+23.csv.gz'
                      ,'ccl-inputs-globe-122808+23.csv.gz'
                      ,'ccl-inputs-globe-122832+23.csv.gz'
                      ,'ccl-inputs-globe-122856+23.csv.gz'
                      ,'ccl-inputs-globe-122880+23.csv.gz'
                      ,'ccl-inputs-globe-122904+23.csv.gz']
        
    if False:
        fnames='ccl-inputs-globe-122736+23.csv.gz'

    file_names = [base+fname for fname in fnames]

    #######################################################################v[2]####
    # Load

    # precsno_arr, visibility_arr = load_for_ccl_inputs(file_name)

    # arrays         = load_for_ccl_inputs(file_name)

    arrays_as_loaded = [ load_for_ccl_inputs(file_name,[('precsno',np.nan,np.float),('visibility',np.nan,np.float)])\
                         for file_name in file_names ]

    precsno_arr        = []
    visibility_arr     = []
    
    if False:
        arrays         = arrays_as_loaded[0]
        precsno_arr    = arrays['precsno']
        visibility_arr = arrays['visibility']

    if False:
        arrays         = arrays_as_loaded[1]
        precsno_arr    = precsno_arr    + arrays['precsno']
        visibility_arr = visibility_arr + arrays['visibility']

    if True:
        for i in range(len(arrays_as_loaded)):
            precsno_arr    = precsno_arr    + arrays_as_loaded[i]['precsno']
            visibility_arr = visibility_arr + arrays_as_loaded[i]['visibility']        

    base_dir='/home/mrilee/nobackup/tmp/others/'

    ###########################################################################
    # Support files
    
    with open(base_dir+'lat_merra2.csv','r') as f_in:
        f_in.readline()
        tmp_f_in = map(lambda x: float(x.split(',')[1]),f_in.readlines())
    lat_merra2 = tmp_f_in
    
    with open(base_dir+'lon_merra2.csv','r') as f_in:
        f_in.readline()
        tmp_f_in = map(lambda x: float(x.split(',')[1]),f_in.readlines())
    lon_merra2 = tmp_f_in
    
    with open(base_dir+'area_merra2.csv','r') as f_in:
        f_in.readline()
        tmp_f_in = map(lambda x: float(x.split(',')[1]),f_in.readlines())
    area_merra2 = tmp_f_in

    ###########################################################################
    # CCL test
    
    k    = 0
    lat  = lat_merra2
    lon  = lon_merra2
    data = precsno_arr[k]
    
    if False:
        m = Basemap(projection='cyl', resolution='l')
        m.drawcoastlines(linewidth=0.5)
        m.pcolormesh(lon,lat,data,latlon=True,vmin=0.0,vmax=1.0e-4)
        plt.show()
    
    print 'initializing ccl_stack'
    ccl_stack = ccl_marker_stack()
    print 'making ccl_stack'
    print 'precsno_arr[0:1] mnmx: ',np.nanmin(precsno_arr[0:1]),np.nanmax(precsno_arr[0:1])
    # thresh_mnmx=(1.0e-5,np.nanmax(precsno_arr[0:1]))
    thresh_mnmx=(1.0e-4,np.nanmax(precsno_arr[0:1]))
    
    # markers = ccl_stack.make_labels_from(precsno_arr[0:1],thresh_mnmx)
    # markers = ccl_stack.make_labels_from(precsno_arr[0:4],thresh_mnmx)
    markers = ccl_stack.make_labels_from(precsno_arr,thresh_mnmx)

    if False:
        print '---markers---'
        np.set_printoptions(threshold=5000,linewidth=300)
        print 'markers[0] aka m_results_translated[0:60,0:60]\n',markers[0][0:60,0:60]
        print 'markers[1] aka m_results_translated[0:60,0:60]\n',markers[1][0:60,0:60]
        print 'markers[2] aka m_results_translated[0:60,0:60]\n',markers[2][0:60,0:60]
        np.set_printoptions(threshold=1000,linewidth=75)
        print 'markers[0].shape: ',markers[0].shape
        print 'markers[0] type:   ',type(markers[0])
        print 'markers[0] max:   ',np.nanmax(markers[0])

    print '---visibility---'
    np.set_printoptions(threshold=5000,linewidth=300,precision=1,infstr='-')
    tmp = visibility_arr[0][0:60,0:60]
    # tmp[np.where(tmp == 9998.0)] = np.inf
    tmp = tmp/1000.0
    print 'visibility_arr[0][0:60,0:60]\n',tmp
    np.set_printoptions(threshold=1000,linewidth=75,precision=8,infstr='inf')
    print 'len(visibility_arr) =    ',len(visibility_arr)
    print 'visibility_arr[0] mnmx = ',np.nanmin(visibility_arr[0]),np.nanmax(visibility_arr[0])

    # vis_thresh=(1.5,2.5)
    vis_thresh=(1.0e-3,1.0) # This is threshold and repl. value.
    visibility_arr_1000 = []
    nlat = 361
    nlon = 576
    for i in range(len(visibility_arr)):
        # v = np.full((nlat,nlon),0,np.int)
        v = np.full((nlat,nlon),0,np.float)
        # v[np.where(visibility_arr[i] < 1000.0)] = 800.0
        # v[np.where(visibility_arr[i] > 1000.0)] = 10000.0
        v[:,:] =  1.0/visibility_arr[i][:,:]
        visibility_arr_1000.append(v)

    ###########################################################################
        
    print '---visibility-1000---'
    np.set_printoptions(threshold=5000,linewidth=300,precision=0)
    print 'visibility_arr_1000[0][0:60,0:60]\n',visibility_arr_1000[0][0:60,0:60]
    np.set_printoptions(threshold=1000,linewidth=75,precision=8)    
    print 'len(visibility_arr_1000) =    ',len(visibility_arr_1000)
    print 'visibility_arr_1000[0] mnmx = ',np.nanmin(visibility_arr_1000[0]),np.nanmax(visibility_arr_1000[0])

    ###########################################################################
    
    vis_ccl_stack = ccl_marker_stack()
    vis_markers = vis_ccl_stack.make_labels_from(visibility_arr_1000,vis_thresh)
    # vis_markers = vis_ccl_stack.make_labels_from(visibility_arr_1000[0:4],vis_thresh)

    if True:
        np.set_printoptions(threshold=5000,linewidth=300)
        print 'vis_markers[0] aka m_results_translated[0:60,0:60]\n',vis_markers[0][0:60,0:60]
        print 'vis_markers[1] aka m_results_translated[0:60,0:60]\n',vis_markers[1][0:60,0:60]
        print 'vis_markers[2] aka m_results_translated[0:60,0:60]\n',vis_markers[2][0:60,0:60]
        print 'vis_markers[3] aka m_results_translated[0:60,0:60]\n',vis_markers[3][0:60,0:60]
        np.set_printoptions(threshold=1000,linewidth=75)
    print 'vis_markers[0].shape: ',vis_markers[0].shape
    print 'vis_markers[0] type:   ',type(vis_markers[0])
    print 'vis_markers[0] max:   ',np.nanmax(vis_markers[0]),np.nanmin(visibility_arr_1000[0]),np.nanmax(visibility_arr_1000[0])
    print 'vis_markers[0] max:   ',np.nanmax(vis_markers[1]),np.nanmin(visibility_arr_1000[1]),np.nanmax(visibility_arr_1000[1])
    print 'vis_markers[0] max:   ',np.nanmax(vis_markers[2]),np.nanmin(visibility_arr_1000[2]),np.nanmax(visibility_arr_1000[2])
    print 'vis_markers[0] max:   ',np.nanmax(vis_markers[3]),np.nanmin(visibility_arr_1000[3]),np.nanmax(visibility_arr_1000[3])

    

    
    
    
    
