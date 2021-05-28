;
;
;
run pgm = hwynet

neti[1] = ..\output\mtc_final_network_loaded.net

neto = ..\output\mtc_final_network_loaded_assigned.net
;FILEO linko = ..\output\mtc_final_network_loaded_assigned_links.shp FORMAT=SHP
;FILEO nodeo = ..\output\mtc_final_network_loaded_assigned_nodes.shp FORMAT=SHP


; combine the records of the time-period-specific assignments
merge record = t

; linkmerge phase start
phase = linkmerge

  HAS_VOL = 0
  IF (VOL24HR_TOT > 0)  HAS_VOL = 1
  IF (VOLDIST_SCLR > 0)   HAS_VOL = 1
  IF (VOLTIME_SCLR > 0)   HAS_VOL = 1
  ASSIGNABLE = 0
  IF (HAS_VOL == 1) ASSIGNABLE = 1
  ; Keep all facility types in expressway, freeway, freeway-to-freeway/ramp, divided and undivided arterials
  IF (FT <= 5) ASSIGNABLE = 1

endphase

endrun
