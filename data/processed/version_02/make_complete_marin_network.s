RUN PGM = NETWORK
FILEI NETI = "complete_network.net"
FILEO NETO = "complete_marin_network.net"

COUNTY = LTRIM(TRIM(COUNTY)) 
CNTYPE = LTRIM(TRIM(CNTYPE))

IF (COUNTY!='Marin') DELETE

ENDRUN