

import numpy as np
# from load_for_ccl_inputs import load_for_ccl_inputs
from load_ncdf_for_matsuzawa import input_array, m2_datafiles

from ccl_marker_stack    import ccl_dask
from time                import time

M2T1NXFLX_base,M2T1NXFLX_datafiles\
    = m2_datafiles(M2DATASET='M2T1NXFLX')

nfiles = len(M2T1NXFLX_datafiles)

run_case = 'TestCase1'
# run_case = 'TestCase2'
# run_case = 'TestCase3-1'
# run_case = 'TestCase3-2'
# run_case = 'TestCase3-3'
# run_case = 'TestCase3-4'
# run_case = 'TestCase4'
run_case_set = False
if 'run_results' not in locals() and 'run_results' not in globals():
    run_results = {}

# Test case 1.
if run_case == 'TestCase1':
    run_case_set = True
    findex_ranges=[\
                   [0],\
                   [1] \
    ]
# 89
    
# Test case 2.
if run_case == 'TestCase2':
    run_case_set = True
    findex_ranges=[\
                   [0,1]\
    ]
# 89

if run_case == 'TestCase3-1':
    run_case_set = True
    findex_ranges=[\
                   [0,1,2]\
    ]

if run_case == 'TestCase3-2':
    run_case_set = True
    findex_ranges=[\
                   [0],\
                   [1,2]\
    ]

if run_case == 'TestCase3-3':
    run_case_set = True
    findex_ranges=[\
                   [0,1],\
                   [2]\
    ]

if run_case == 'TestCase3-4':
    run_case_set = True
    findex_ranges=[\
                   [0]\
                   ,[1]\
                   ,[2]\
    ]

if run_case == 'TestCase4':
    run_case_set = True
    findex_ranges=[\
                   [0,1]\
                   ,[2,3]\
                   ,[4,5]\
                   ,[6,7]\
    ]


if run_case_set is False:
    print 'run_case is not set, quitting!'
    quit()

def loader(fidx_range,argList):
    return input_array(fidx_range).visibility_i[:]

###########################################################################
# Start timing

time_force_execution            = False
time_measurements               = {}
time_measurements['time_start'] = time()

###########################################################################
# Load

# precsno_arr, visibility_arr = load_for_ccl_inputs(file_name)

# For extinction, 1/visibility.
thresh_mnmx = (1.0e-3,1.0)

# Calculation: Load data
if True:
    ccl_dask_object = ccl_dask()
    ccl_dask_object.load_data_segments_with_loader(loader,findex_ranges,[()])

if time_force_execution:
    for future in ccl_dask_object.data_seg:
        future.result()
time_measurements['time_loader_1'] = time()

# Diagnostics
if False:
    print 'ccl_dask_object.data_segs',ccl_dask_object.data_segs
    print 'execute'
    ccl_dask_object.data_segs[0].result()
    print 'ccl_dask_object.data_segs',ccl_dask_object.data_segs
    
# Calculations
if True:
    ccl_dask_object.make_blizzard_stacks(thresh_mnmx)
    # ccl_dask_object.make_stacks(thresh_mnmx)

    if time_force_execution:
        for future in ccl_dask_object.segs_blizz_mask_results:
            future.result()
    time_measurements['time_blizz_mask_results_1'] = time()
    
    ccl_dask_object.shift_labels()
    ccl_dask_object.make_translations()
    ccl_dask_object.apply_translations()

    # serial collect
    ccl_dask_object.collect_all_results()

    # Simplify the labels so they are sequential without gaps.
    print 'simplify_labels'
    ccl_dask_object.simplify_labels()

    # serial collect
    print 'collect_simplified_results'
    ccl_dask_object.collect_simplified_results()

    time_measurements['time_ccl_simplified_results_1'] = time()
    
    print 'ccl_dask calculation done'

# Diagnostics looking at the results of the data load
if False:
    print 'ccl_dask_object.data_segs[0].results()[0]\n'\
        ,ccl_dask_object.data_segs[0].result()[0]

if True:
    iseg = 0
    isl = 0
    rc  = 60
    r0  = 0
    c0  = 0
    r1  = r0 + rc
    c1  = c0 + rc

# Original default
if False:
    iseg = 0
    isl = 0
    rc  = 60
    r0  = 0
    c0  = 0
    r1  = r0 + rc
    c1  = c0 + rc

# Diagnostics look at the results with the working labels
if True:
    np.set_printoptions(threshold=5000,linewidth=600)
    print 'ccl_dask_object len(ccl_results): ',len(ccl_dask_object.ccl_results)
    print 'ccl_dask_object.ccl_results[iseg].m_results_translated[isl][r0:r1,c0:c1]'\
        ,isl,r0,r1,c0,c1,'\n'\
        ,ccl_dask_object.ccl_results[iseg].m_results_translated[isl][r0:r1,c0:c1]
    np.set_printoptions(threshold=1000,linewidth=75)
    # For Test case 3.
    #   Without the 3-hour blizzard masking, we get 1214 showing in the 60x60 view.
    #   With the 3-hour masking, we get 1203.

# Diagnostics for simplified labels
# object.ccl_results_simplified_labels has the results from the futures in ooooooooooooooobject.ccl_stacks_simplified_b
if True:
    np.set_printoptions(threshold=5000,linewidth=600)
    print 'ccl_dask_object.ccl_results_simplified_labels[iseg].m_results_simple[isl][r0:r1,c0:c1]'\
        ,isl,r0,r1,c0,c1,'\n'\
        ,ccl_dask_object.ccl_results_simplified_labels[iseg].m_results_simple[isl][r0:r1,c0:c1]
    np.set_printoptions(threshold=1000,linewidth=75)

if False:
    ccl_dask_object.diagnose_parallel_simplify_0()

if True:    
    ccl_dask_object.close()

print 'ccl_dask_blizzard_nc4 completing '+run_case
run_results[run_case] = ccl_dask_object
print 'wall clock timings'
print 'load                ',time_measurements['time_loader_1']                 - time_measurements['time_start']
print 'blizz mask+load     ',time_measurements['time_blizz_mask_results_1']     - time_measurements['time_start']
print 'ccl+blizz mask+load ',time_measurements['time_ccl_simplified_results_1'] - time_measurements['time_start']
print 'blizz mask          ',time_measurements['time_blizz_mask_results_1']     - time_measurements['time_loader_1']
print 'ccl                 ',time_measurements['time_ccl_simplified_results_1'] - time_measurements['time_blizz_mask_results_1']
print 'done'

# Note, if we have to do the 3-hour blizzard calculation w/o CCL, then we can monkey with the load_data_segments to
# have files loaded onto separate cluster nodes, like ghost cells. Alternatively, we can Dask it by client.submitting
# tasks with dependencies on those two adjacent futures.



