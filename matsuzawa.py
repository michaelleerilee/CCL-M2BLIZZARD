
from math import sqrt

def matsuzawa_blizzard_mask(precsno,prectot,u10m,v10m,u2m,v2m,frland,frlandice):
    # mask_all_snow, iif (precsno > 0.  AND precsno = prectot AND (frland+frlandice) >= 0.5 , int8(1) , null)
    mask_all_snow = ( precsno > 0. and precsno == prectot and (frland+frlandice) >= 0.5 )
    
    # visibility = 707.95*(pow(s2m*(fact3+(fact2*fact1)),-0.773 ))*mask_all_snow
    # if mask_all_snow > 0
    if mask_all_snow:

        s10m = sqrt( (u10m*u10m) + (v10m * v10m) )
        s2m  = sqrt( (u2m*u2m) + (v2m * v2m) )
        fact1 = pow ( (2.0/0.15), (-0.35/(0.4*0.036*s10m) ) )
        fact2 = 30.0 - (precsno*1000./1.2)
        fact3 = precsno*1000./1.2
        
        visibility = 707.95*(pow(s2m*(fact3+(fact2*fact1)),-0.773 ))
        extinction_coeff = 3.912/visibility,
        mask_visib_800   = visibility <=  800
        mask_visib_1000  = visibility <=  1000
        mask_visib_1200  = visibility <=  1200
    else:
        visibility       = np.inf
        extinction_coeff = 0.0
        mask_visib_800   = False
        mask_visib_1000  = False
        mask_visib_1200  = False
            
    # save to MERRA2_visib_with_land_landice
    return mask_all_snow,visibility,mask_visib_800,mask_visib_1000,mask_visib_1200


