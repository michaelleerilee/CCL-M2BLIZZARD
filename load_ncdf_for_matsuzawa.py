
# M2T1NXFLX, precsno, prectot
# M2T1NXSLV, u10m, v10m, u2m, v2m
# M2C0NXASM, frland, frlandice
# <frland:float null, frlandice:float null>[z=0:0,1,0,y=0:360,91,0,x=0:575,144,0]


try:
    M2T1NXFLX_
    M2T1NXFLX_arrays=['PRECSNO','PRECTOT']
    M2T1NXFLX_file_name=''
    M2T1NXFLX_ds=Dataset(M2T1NXFLX_name)

    M2T1NXSLV_
    M2T1NXSLV_arrays='U10M,V10M,U2M,V2M'.split(',')
    M2T1NXSLV_file_name=''
    M2T1NXSLV_ds=Dataset(M2T1NXSLV_name)

    M2CONXASM_
    M2CONXASM_arrays=['frland', 'frlandice']
    M2CONXASM_file_name=''
    M2CONXASM_ds=Dataset(M2CONXASM_name)
except:
    print 'ERROR'

    
