from numpy import int32
import pandas as pd


def create_trn_links(transit_lines):
    """
    Parse through the transit node sequences, and create transit links from Cube transit line file.

    Parameter:
        transit_lines: path to a Cube transit line file.

    Return:
        A pandas dataframe with 3 columns:
        - line_name: name of the transit line
        - A: node A ID number
        - B: node B ID number
    """

    with open(transit_lines, "r") as f:
        fline = f.readline()

        # initialize status variables
        node_info_start = False
        line_name = ""
        node_A = 0
        node_B = 0
        line_links = {"line_name": [], "A": [], "B": []}

        # initialize an empty df for links info
        trn_links = pd.DataFrame()
        print(trn_links)

        while fline:

            # retrieve transit line name
            if fline.startswith("LINE NAME"):
                line_name = (
                    fline.split("=")[1].strip().replace('"', "").replace(",", "")
                )

            # when reaches the end of each transit line
            if fline == "\n":
                # convert line_links to df, and append the df to trn_links
                line_links_df = pd.DataFrame.from_dict(line_links)
                trn_links = trn_links.append(line_links_df, ignore_index=True)

                # reset status variables
                node_info_start = False
                line_name = ""
                node_A = 0
                node_B = 0
                line_links = {"line_name": [], "A": [], "B": []}

            if node_info_start:
                node = abs(
                    int(fline.split(",")[0])
                )  # use absolute value to capture non-stop transit nodes
                if node_A == 0 and node_B == 0:  # 1st node of the transit line
                    node_A = node
                elif node_B == 0:  # 2nd node of the transit line
                    node_B = node
                else:
                    node_A = node_B
                    node_B = node
                # print(f"A: {node_A}, B: {node_B}")

                if node_B != 0:  # if not the 1st node
                    line_links["line_name"].append(line_name)
                    line_links["A"].append(node_A)
                    line_links["B"].append(node_B)

            if fline.startswith(" N="):
                print(f"start parsing node info for line {line_name}")
                node_info_start = True

            fline = f.readline()

        # assert dtype
        dtype_dict = {"line_name": str, "A": int32, "B": int32}
        trn_links = trn_links.astype(dtype_dict)

        # return trn_links
        return trn_links


def validate_trn_links(network_AB, trn_links):
    """
    Check if transit links is valid (if it exist in network).

    Parameters:
        network_AB: A dataframe with network links A, B nodes.
        trn_links: A dataframe of transit links A, B nodes.
                  (can be created by the `create_trn_links` function)

    Return:
        a dataframe of transit links that cannot be found in the network links.
    """

    net = network_AB.copy()
    net["in_net"] = 1  # indicator variable to flag that the link is in network

    validate_df = pd.merge(trn_links, net, on=["A", "B"], how="left")

    # filter out transit links that do not exist in network
    issue = validate_df[validate_df["in_net"].isnull()].drop(columns=["in_net"])

    return issue


if __name__ == "__main__":
    network_AB = pd.read_csv("data/network_AB_v7.csv")
    trn_links = create_trn_links("data/transitLines_ver7.lin")
    issue = validate_trn_links(network_AB, trn_links)
    print(issue.shape)
    print(issue.head(20))
    issue.to_csv("data/transit_node_seq_issue.csv", index=False)