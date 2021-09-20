import pandas as pd

# initialize an empty list, will be added with dictionaries in the format:
# {"operator": operator, "route_id": route_id, "stop_node": node}
station_nodes = []

# parse through transit line file
# for each route that is either BART or Caltrain, collect its stop nodes
with open("data/transitLines.lin", "r") as f:
    line = f.readline()

    # initialize status variables
    operator = ""
    route_id = 0
    station_info_start = False
    is_bart_or_caltrain = False

    while line:

        if line.startswith("LINE NAME"):
            route_name = line.split("=")[1].strip().replace(",", "")
            route_id = int(route_name.split("_")[1])

        if line.startswith(" OPERATOR"):
            operator_id = int(line.split("=")[1].strip().replace(",", ""))
            if operator_id == 26:
                operator = "BART"
                is_bart_or_caltrain = True
            elif operator_id == 17:
                operator = "Caltrain"
                is_bart_or_caltrain = True

        if line == "\n":  # end of the stop node info
            station_info_start = False
            is_bart_or_caltrain = False

        if station_info_start and is_bart_or_caltrain:
            node = int(line.strip().replace(",", ""))
            if node > 0:  # means it's a stop, not just an intermediate node
                station_nodes.append(
                    {"operator": operator, "route_id": route_id, "stop_node": node}
                )

        if line.startswith(" N="):
            station_info_start = True

        # move to next line
        line = f.readline()


# convert to dataframe and export results
station_df = pd.DataFrame.from_dict(station_nodes).drop_duplicates()
bart_station_df = station_df[station_df["operator"] == "BART"].reset_index(drop=True)
caltrain_station_df = station_df[station_df["operator"] == "Caltrain"].reset_index(
    drop=True
)
bart_station_df.to_csv("data/bart_station_nodes.csv", index=False)
caltrain_station_df.to_csv("data/caltrain_station_nodes.csv", index=False)