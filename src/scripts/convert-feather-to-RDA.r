#
# Simple feather to RDA converter
# (Useful since Tableau doesn't read feather)
# See https://www.rstudio.com/blog/feather/
#
# USAGE: RScript --vanilla convert-feather-to-RDA.r input.feather output.rda
#
library("arrow")
args = commandArgs(trailingOnly=TRUE)

if (length(args) != 2) {
    stop("Two arguments required: input.feather and output.rda")
}
print(paste("Reading input file:", args[1]))
input_df  <- read_feather(args[1])

print(paste("Dataframe has",nrow(input_df),"rows and",ncol(input_df),"columns"))
print(paste("Writing output file:", args[2]))
save(input_df, file = args[2])
