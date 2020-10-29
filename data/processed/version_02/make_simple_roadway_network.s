RUN PGM = NETWORK
FILEI NETI = "complete_network.net"
FILEO NETO = "simple_roadway_network.net"

COUNTY = LTRIM(TRIM(COUNTY)) 
CNTYPE = LTRIM(TRIM(CNTYPE))

IF (FT>5) DELETE

ENDRUN