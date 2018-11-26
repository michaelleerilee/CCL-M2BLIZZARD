

import numpy as np
# from load_for_ccl_inputs import load_for_ccl_inputs
from load_ncdf_for_matsuzawa import input_array, m2_datafiles

from ccl_marker_stack    import ccl_dask

M2T1NXFLX_base,M2T1NXFLX_datafiles\
    = m2_datafiles(M2DATASET='M2T1NXFLX')

nfiles = len(M2T1NXFLX_datafiles)

# Test case 1.
findex_ranges=[\
               [0],\
               [1] \
               ]

# quit()

def loader(fidx_range,argList):
    return input_array(fidx_range).visibility_i[:]

###########################################################################
# Load

# precsno_arr, visibility_arr = load_for_ccl_inputs(file_name)

# For extinction, 1/visibility.
thresh_mnmx = (1.0e-3,1.0)

# The calculation
if True:
    ccl_dask_object = ccl_dask()
    ccl_dask_object.load_data_segments_with_loader(loader,findex_ranges,[()])

# Diagnostics
if False:
    print 'ccl_dask_object.data_segs',ccl_dask_object.data_segs
    print 'execute'
    ccl_dask_object.data_segs[0].result()
    print 'ccl_dask_object.data_segs',ccl_dask_object.data_segs
    

if True:
    ccl_dask_object.make_blizzard_stacks(thresh_mnmx)
    # ccl_dask_object.make_stacks(thresh_mnmx)
    ccl_dask_object.shift_labels()
    ccl_dask_object.make_translations()
    ccl_dask_object.apply_translations()

if False:
    print 'ccl_dask_object.data_segs[0].results()[0]\n'\
        ,ccl_dask_object.data_segs[0].result()[0]

if True:
    np.set_printoptions(threshold=5000,linewidth=600)
    print 'ccl_dask_object.ccl_results[0].m_results_translated[0][0:60,0:60]\n'\
        ,ccl_dask_object.ccl_results[0].m_results_translated[0][0:60,0:60]
    np.set_printoptions(threshold=1000,linewidth=75)
    # For Test case 3.
    #   Without the 3-hour blizzard masking, we get 1214 showing in the 60x60 view.
    #   With the 3-hour masking, we get 1203.
    
ccl_dask_object.close()


# Note, if we have to do the 3-hour blizzard calculation w/o CCL, then we can monkey with the load_data_segments to
# have files loaded onto separate cluster nodes, like ghost cells. Alternatively, we can Dask it by client.submitting
# tasks with dependencies on those two adjacent futures.



