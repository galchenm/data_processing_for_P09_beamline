; Optimized panel offsets can be found at the end of the file
; Optimized panel offsets can be found at the end of the file
; Optimized panel offsets can be found at the end of the file
; Manually optimized with hdfsee
; Manually optimized with hdfsee

clen = $DETECTOR_DISTANCE
photon_energy = $PHOTON_ENERGY
;max_adu = 100000

adu_per_photon = 1
res = 18181.8   ; 55 micron pixel size

; These lines describe the data layout for the Eiger native multi-event files
dim0 = %
dim1 = ss
dim2 = fs
data = /entry/data/data

; Uncomment these lines if you have a separate bad pixel map (recommended!)
;mask_file = /asap3/petra3/gpfs/p09/2022/data/11013673/processed/yefanov/mask/mask_80mm_grid_v0.h5
;mask = /data/data
;mask_good = 0x1
;mask_bad = 0x0

rigid_group_panel0 = panel0
rigid_group_panel1 = panel1
rigid_group_d0 = panel0,panel1
rigid_group_collection_panels = panel0,panel1
rigid_group_collection_det = d0

; corner_{x,y} set the position of the corner of the detector (in pixels)
; relative to the beam

;bad_panel0/min_fs = 0 
;bad_panel0/min_ss = 0 
;bad_panel0/max_fs = 1555
;bad_panel0/max_ss = 515
;bad_panel0/panel = panel0

;bad_panel1/min_fs = 0 
;bad_panel1/min_ss = 516 
;bad_panel1/max_fs = 1555
;bad_panel1/max_ss = 1031
;bad_panel1/panel = panel1


panel0/min_fs = 0 
panel0/min_ss = 0 
panel0/max_fs = 1555
panel0/max_ss = 515
panel0/corner_x = 684.468813
panel0/corner_y = -701.859709
panel0/fs = -0.000496x +1.000000y
panel0/ss = -1.000000x -0.000496y

panel1/min_fs = 0 
panel1/min_ss = 516 
panel1/max_fs = 1555
panel1/max_ss = 1031
panel1/corner_x = 32.424113
panel1/corner_y = -705.774709
panel1/fs = -0.000629x +1.000000y
panel1/ss = -1.000000x -0.000629y





panel0/coffset = 0.000113
panel1/coffset = -0.000041
