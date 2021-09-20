; Reading in transit lines and exporting a link file

     run pgm = PUBLIC TRANSPORT

        ;FILEI NETI = "mtc_transit_network_AM_SET1_with_transit.net"
        FILEI NETI = ..\output\mtc_final_network_loaded.net
        ;FILEI LINEI[1] = ..\model_run\trn\transitLines_new_nodes.lin
        FILEI LINEI[1] = ..\model_run\trn\transitLines.lin
        FILEI SYSTEMI = ..\model_run\trn\transitSystem.PTS
        FILEI FACTORI[1] = ..\model_run\trn\transitFactors_SET1.fac

        FILEI FAREMATI[1] = ..\model_run\trn\fareMatrix.txt
        FILEI FAREI= ..\model_run\trn\fares.far

        FILEO NETO = ..\output\mtc_final_network_loaded_flagged_with_transit.net
        ;FILEO MATO[1] = "transit_skims_AM_SET1.TPP",
        ;                         MO=2-17,
        ;                         NAME = COMPCOST,IWAIT,XWAIT,XPEN,BRDPEN,XFERS,FARE,XWTIME,AEWTIME,
        ;                                LB_TIME,EB_TIME,FR_TIME,LR_TIME,HR_TIME,CR_TIME,BEST_MODE
        ;FILEO LINEO = "mtc_transit_lines_AM_SET1_with_transit.lin"
        ;FILEO NTLEGO = "mtc_transit_ntlegs_AM_SET1_with_transit.ntl"
        ;FILEO ROUTEO[1] = "mtc_transit_routes_AM_SET1_with_transit.rte"


        FILEO REPORTO = "mtc_transit_report_AM_SET1_with_transit.rpt"
        FILEO LINKO = test.dbf NTLEGS=N



        PARAMETERS NOROUTEERRS=17000000
        PARAMETERS TRANTIME=LI.CTIM
        PARAMETERS MAPSCALE=5280

        PROCESS PHASE=LINKREAD
            LW.TRANTIME = LI.CTIM
            LW.DISTANCE = LI.FEET/5280
        ENDPROCESS

        PHASE=DATAPREP
          ;access/egress links
           GENERATE,
              NTLEGMODE=991,
              INCLUDELINK=(LI.CNTYPE='TAP'),
              COST=1,
              MAXCOST=999*500,
              ONEWAY=T
           ;transfer links
           ;GENERATE,
            ;  NTLEGMODE=992,
            ;  INCLUDELINK=(LI.NTL_MODE=2),
            ;  COST=LI.WALKTIME,
            ;  MAXCOST=999*500,
            ;  DIRECTLINK = 3,
            ;  FROMNODE=1-10000000,
            ;  TONODE=1-10000000
      ENDPHASE


    ENDRUN
