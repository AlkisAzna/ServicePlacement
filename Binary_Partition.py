######################################################
# Partition algorithm to produce clusters given specific thresholds of cpu and ram usage
# Input: Service List, Service Affinities
# Output: Partition of application ready to be packed into host machines
# Paper: Optimizing Service Placement for MicroserviceArchitecture in Clouds
######################################################
import copy
import random


class Binary_Partition:
    def __init__(self, pod_request_cpu, pod_request_ram, service_affinities, max_ram_allocation, max_cpu_allocation, host_list, service_list):
        self.pod_request_cpu = copy.deepcopy(pod_request_cpu)
        self.pod_request_ram = copy.deepcopy(pod_request_ram)
        self.service_affinities = copy.deepcopy(service_affinities)
        self.max_ram_allocation = float(max_ram_allocation)
        self.max_cpu_allocation = float(max_cpu_allocation)
        self.host_list = host_list
        self.service_list = service_list
        self.app_partition = {}

    @staticmethod
    def graph_construction(services, affinities):
        from collections import defaultdict
        # function for adding edge to graph
        graph = defaultdict(list)

        def addEdge(curr_graph, u, v):
            curr_graph[u].append(v)

        # definition of function
        def generate_edges(curr_graph):
            edges = []

            for node in curr_graph:
                # for each neighbour node of a single node
                for neighbour in curr_graph[node]:
                    # if edge exists then append
                    edges.append((node, neighbour))
            return edges

        # declaration of graph as dictionary
        for source in affinities:
            for dest in affinities[source]:
                if source in services and dest in services:
                    addEdge(graph, source, dest)

        return graph

    @staticmethod
    def contract_graph(parts, temp_graph, affinities):
        curr_graph = copy.deepcopy(temp_graph)

        # Total Edjes
        edje_count = 0
        for x in temp_graph:
            edje_count += len(temp_graph[x])

        while edje_count > (
                parts - 1):  # For Binary Partition we need 2 Vertices and 1 Edje - K partition -> K Vertices and K-1 Edjes(at least)
            # Pick random source and destination whose affinity hasnt be processed
            random_source = random.choice(list(curr_graph.keys()))
            random_dest = random.choice((curr_graph[random_source]))

            while float(affinities[random_source][random_dest]) == 0.0:
                random_source = random.choice(list(curr_graph.keys()))
                random_dest = random.choice((curr_graph[random_source]))

            # Check if Random_Dest is also a source and update all the destination services for random_source
            if random_dest in curr_graph:
                for dest in curr_graph[random_dest]:

                    # Check if source contains the specific dest - otherwise add service and affinity
                    if dest in curr_graph[random_source]:
                        affinities[random_source][dest] = format(
                            float(float(affinities[random_source][dest]) + float(affinities[random_dest][dest])), '.4f')
                        # Decrease Edjes
                        edje_count -= 1
                    else:
                        if dest == random_source:
                            if len(curr_graph) != 2 or edje_count != 2:
                                edje_count -= 1
                            continue
                        else:
                            # Append Service and Add Affinity
                            curr_graph[random_source].append(dest)
                            affinities[random_source][dest] = format(float(affinities[random_dest][dest]), '.4f')

                    # Remove affinity
                    affinities[random_dest].pop(dest)
                curr_graph[random_dest].clear()

            # Search if random_dest has other affinities with other sources
            for key in curr_graph:
                if key == random_source:
                    continue
                else:
                    # Check if dest service is also in sources
                    if random_dest in curr_graph and key == random_dest:
                        continue
                    else:
                        # Dest in other affinity sources
                        if random_dest in curr_graph[key]:
                            # Random Source not in current source affinities -> add service and finally add affinity
                            if random_source in curr_graph[key]:
                                affinities[key][random_source] = format(
                                    (float(affinities[key][random_source]) + float(affinities[key][random_dest])),
                                    '.4f')
                                # Decrease Edjes
                                edje_count -= 1
                            else:
                                curr_graph[key].append(random_source)
                                affinities[key][random_source] = format(float(affinities[key][random_dest]), '.4f')
                            affinities[key].pop(random_dest)
                            curr_graph[key].remove(random_dest)

            # Update Source affinity - Remove Dest Service
            if len(curr_graph) != 2 or edje_count != 2:
                curr_graph[random_source].remove(random_dest)
                affinities[random_source][random_dest] = '0.0'  # Empty affinity - Means that source contains dest

            # Remove the Empty Dest if Exists
            if random_dest in curr_graph:
                # Check if random source contained other sources
                if bool(affinities[random_dest]):
                    for key in affinities[random_dest]:
                        if key not in affinities[random_source] and key != random_source:
                            affinities[random_source][key] = '0.0'

                # Update Graph
                curr_graph.pop(random_dest)
                affinities.pop(random_dest)

            # Check for empty source
            if not bool(curr_graph[random_source]):
                curr_graph.pop(random_source)

            # Decrease Edjes
            edje_count -= 1

        # Update the partition
        app_partition = {}
        # Check for empty affinities
        if len(curr_graph) != len(affinities):
            host_service = ''
            empty_host = ''
            for key in affinities:
                if key not in curr_graph:
                    empty_host = key
                else:
                    host_service = key

            # Check the empty host services
            for key in affinities[empty_host]:
                if key not in affinities[host_service]:
                    affinities[host_service][key] = '0.0'
            if empty_host not in affinities[host_service]:
                affinities[host_service][empty_host] = '0.0'
            affinities.pop(empty_host)

        curr_graph = copy.deepcopy(affinities)
        for source in curr_graph:
            app_partition[source] = []
            for dest in curr_graph[source]:
                if curr_graph[source][dest] != '0.0':
                    app_partition[dest] = []
                else:
                    app_partition[source].append(dest)

        return app_partition

    # Calculate the partitions of Application for given alpha value (percentage of resources usage)
    def calculate_app_partitions(self, alpha):
        # Initialization
        curr_partition = {'1': copy.deepcopy(self.service_list)}
        total_parts = len(curr_partition)
        k_partition = 2

        # Iterate until we find a suitable partition
        while True:
            app_partition = copy.deepcopy(curr_partition)
            # Gather Resource demands and Number of Services
            for part in app_partition:
                sum_cpu_usage = 0.0
                sum_ram_usage = 0.0
                check_resource_demands = True
                check_number_of_services = True

                # Check if part contains more than one service
                if len(app_partition[part]) <= 1:
                    check_number_of_services = False

                # Check resource demands and if they exceed alpha
                for service in app_partition[part]:
                    temp_cpu = float(self.pod_request_cpu[service])
                    temp_ram = float(self.pod_request_ram[service])

                    sum_cpu_usage += temp_cpu
                    sum_ram_usage += temp_ram

                if (sum_cpu_usage < (self.max_cpu_allocation * alpha) and
                        sum_ram_usage < (self.max_ram_allocation * alpha)):
                    check_resource_demands = False

                # Cannot meet Criteria - Partition Application Part
                if check_number_of_services and check_resource_demands:
                    contraction_repeats = len(app_partition[part])
                    temp_graph = self.graph_construction(app_partition[part], self.service_affinities)
                    min_graph = copy.deepcopy(temp_graph)
                    min_sum = 0.0
                    temp_sum = 0.0

                    # Find part service affinities and min sum
                    part_service_traffic = {}
                    for source in temp_graph:
                        part_service_traffic[source] = {}
                        for dest in temp_graph[source]:
                            part_service_traffic[source][dest] = float(self.service_affinities[source][dest])
                            min_sum += float(self.service_affinities[source][dest])

                    # Remove current part
                    curr_partition.pop(part)

                    # Contraction Algorithm
                    while contraction_repeats > 0:
                        # Apply contraction Algorithm
                        service_traffic = copy.deepcopy(part_service_traffic)
                        partitioned_graph = self.contract_graph(k_partition, temp_graph, service_traffic)

                        # Compare with minimum - Check if null
                        if bool(partitioned_graph):
                            for service in service_traffic:
                                for x in service_traffic[service]:
                                    temp_sum += float(service_traffic[service][x])

                            # Check if another minimum Graph found
                            if temp_sum < min_sum:
                                min_graph = partitioned_graph
                                min_sum = temp_sum

                        # Decrease repeats
                        temp_sum = 0.0
                        contraction_repeats -= 1

                    # Partition the application and repeat process
                    for key in min_graph:
                        curr_partition[str(total_parts + 1)] = min_graph[key]
                        if key not in curr_partition[str(total_parts + 1)]:
                            curr_partition[str(total_parts + 1)].append(key)
                        total_parts += 1

            # Identical dictionaries - No changes happen - Break
            if app_partition == curr_partition:
                break

        self.app_partition = app_partition
