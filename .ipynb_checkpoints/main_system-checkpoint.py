import pandas as pd

def main_system():
    # Read File
    file_config = open('pods_configuration.txt', 'r')
    lines = file_config.readlines()

    rows = []
    cols = []
    count = 0
    # Strips the newline character
    for line in lines:
        # Read line
        data = line.strip()
        # Make the line into an array of chars
        res = [val for idx, val in enumerate(data) if val or (not val and data[idx - 1])]
        temp_string = ""
        temp_row = []
        mono_spaced = False  # To allow single spaced strings
        for index, x in enumerate(res):
            # Check if the next char is blank
            if x == " ":
                # Omit all blank spaces except those who are after non empty strings
                if temp_string != "":
                    if not mono_spaced:
                        mono_spaced = True
                        continue
                    else:
                        if count == 0:
                            cols.append(temp_string)
                        else:
                            temp_row.append(temp_string)
                        mono_spaced = False
                        temp_string = ""
                        continue
            else:
                # Check if only one space exists between two strings
                if mono_spaced:
                    temp_string = temp_string + " " + x
                    mono_spaced = False
                else:
                    temp_string = temp_string + x
        if count == 0:
            cols.append(temp_string)
        else:
            temp_row.append(temp_string)
            rows.append(temp_row)
        count += 1
    # Produce the Dataframe
    dfile = pd.DataFrame(rows)
    for idx, val in enumerate(dfile.columns):
        dfile = dfile.rename(columns={idx: str(cols[idx])})
    cluster_nodes = {}
    # Initialize Dictionary of Cluster Nodes
    for idx, val in enumerate(dfile.loc[:, "NODE"]):
        if val in cluster_nodes:
            continue
        else:
            cluster_nodes[val] = {}

    # Assign Pods to Specific Nodes
    for idx, val in enumerate(dfile.loc[:, "NAME"]):
        curr_node = dfile.loc[idx, "NODE"]
        instances = dfile.loc[idx, "READY"]
        cluster_nodes[curr_node][val] = instances
    print(cluster_nodes)


if __name__ == "__main__":
    main_system()
