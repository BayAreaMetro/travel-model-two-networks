; first do a shortest-path assignment by distance

     run pgm = highway

        ; time-specific input highway network
        neti = ..\input\avgloadAM_taz.net

        ; household travel demand
        mati[1] = ..\input\auto_SCALAR_1.mat

        ; loaded time-specific highway network
        neto = ..\output\distScalar_assigned_tmp.net

        ; set the assignment parameters -- equilibrium assignment, bi-conjugate
        parameters combine = equi, enhance = 2

        ; set the assignment parameters -- internal zones + ?? external zones
        parameters zones = 4688


        ; set the assignment parameters -- other closure criteria (do not use)
        parameters gap = 0, aad = 0, raad = 0, rmse = 0 maxiters=1

        ; set the working variables

         phase = linkread


           linkclass = 1

 					  ; distance is coded as "distance" in the networks in miles

            IF (li.DISTANCE = 0)
                lw.dist = 1.0 / 5280.0
                distance = 1.0 / 5280.0
            ELSE
                lw.dist = li.DISTANCE
                distance = li.DISTANCE
            ENDIF

            ; no cap on capacity
            c=99999
            speed=35
         endphase


        ; set the pathload parameters
        phase = iloop

            ; scalar matrix
            pathload path = lw.dist, vol[1] = mi.1.1

        endphase
   endrun
;
; renumber nodes
;
RUN PGM=NETWORK
    NETI = ..\output\distScalar_assigned_tmp.net
    NETO = ..\output\distScalar_assigned.net

    PHASE = INPUT FILEI=NI.1
        N = OLD_NODE
    ENDPHASE

    PHASE = INPUT FILEI=LI.1
        A = OLD_A
        B = OLD_B
    ENDPHASE

ENDRUN

;
; now do a shortest-path assignment by time
;
     run pgm = highway

        ; time-specific input highway network
        neti = ..\input\avgloadAM_taz.net

        ; household travel demand
        mati[1] = ..\input\auto_SCALAR_1.mat

        ; loaded time-specific highway network
        neto = ..\output\timeScalar_assigned_tmp.net

        ; set the assignment parameters -- equilibrium assignment, bi-conjugate
        parameters combine = equi, enhance = 2

        ; set the assignment parameters -- internal zones + ?? external zones
        parameters zones = 4688


        ; set the assignment parameters -- other closure criteria (do not use)
        parameters gap = 0, aad = 0, raad = 0, rmse = 0 maxiters=1

        ; set the working variables

         phase = linkread


           linkclass = 1

 					  ; distance is coded as "DISTANCE" in the new network and is in miles

            IF (li.DISTANCE = 0)
                lw.dist = 1.0 / 5280.0
                distance = 1.0 / 5280.0
            ELSE
                lw.dist = li.DISTANCE
                distance = li.DISTANCE
            ENDIF

            c=99999
            speed=35
            lw.time= lw.dist/li.FFS * 60
         endphase


        ; set the pathload parameters
        phase = iloop

            ; scalar matrix
            pathload path = lw.time, vol[1] = mi.1.1

        endphase
   endrun
;
; renumber nodes
;
RUN PGM=NETWORK
    NETI = ..\output\timeScalar_assigned_tmp.net
    NETO = ..\output\timeScalar_assigned.net

    PHASE = INPUT FILEI=NI.1
        N = OLD_NODE
    ENDPHASE

    PHASE = INPUT FILEI=LI.1
        A = OLD_A
        B = OLD_B
    ENDPHASE

ENDRUN
;
; merge volume from assignment to network
;
   run pgm = hwynet

   ; does mtc_final_network_base contain VOL24HR_TOT?
   neti[1] = ..\input\mtc_final_network_base.net
   neti[2] = ..\input\msamerge3.net
   neti[3] = ..\output\distScalar_assigned.net
   neti[4] = ..\output\timeScalar_assigned.net

   ; output network includes 24-hour volumes
   neto = ..\output\mtc_final_network_loaded.net, INCLUDE=Name,A,B,DISTANCE,FEET,
   ASSIGNABLE,CNTYPE,BIKE_ACCESS,BUS_ONLY,COUNTY,DRIVE_ACCESS,FT,MANAGED,RAIL_ONLY,
   TOLLSEG,TOLLBOOTH,TRANSIT,WALK_ACCESS,LANES_EA,LANES_AM,LANES_MD,LANES_PM,LANES_EV,
   USECLASS_EA,USECLASS_AM,USECLASS_MD,USECLASS_PM,USECLASS_EV, TRANTIME,
   VOL24HR_TOT,VOLDIST_SCLR,VOLTIME_SCLR


   ; combine the records of the time-period-specific assignments
   merge record = t

   ; linkmerge phase start
   phase = linkmerge

       VOL24HR_TOT  = li.2.VOL24HR_TOT
       VOLDIST_SCLR = li.3.V_1
       VOLTIME_SCLR = li.4.V_1

   endphase

endrun
