######################################################
# Bin Packing algorithm trying to fit partitions together or seperately in given VMs
# Input: Partitions of Application + Resources
# Output: A new placement solution for current problem
# Paper: Optimizing Service Placement for MicroserviceArchitecture in Clouds
######################################################
import copy


class Bin_Packing():
    def __init__(self, app_partition, current_placement, current_node_usage_cpu, current_node_usage_ram,
                 current_node_available_cpu, current_node_available_ram, pod_request_cpu, pod_request_ram, host_list, service_affinities):
        self.app_partition = app_partition
        self.current_node_usage_cpu = copy.deepcopy(current_node_usage_cpu)
        self.current_node_usage_ram = copy.deepcopy(current_node_usage_ram)
        self.current_node_available_cpu = copy.deepcopy(current_node_available_cpu)
        self.current_node_available_ram = copy.deepcopy(current_node_available_ram)
        self.pod_request_cpu = pod_request_cpu
        self.pod_request_ram = pod_request_ram
        self.current_placement = current_placement
        self.service_affinities = service_affinities
        self.host_list = host_list
        self.app_placement = {}

    def heuristic_packing(self):
        # Iterate through all parts
        for part in self.app_partition:
            max_tf = 0.0
            max_ml_ram = 0.0
            max_ml_cpu = 0.0
            max_host = ''
            total_ram = 0.0
            total_cpu = 0.0

            # Calculate Resource Demands
            for service in self.app_partition[part]:
                # Calculate resources for current service
                temp_cpu = float(self.pod_request_cpu[service])
                temp_ram = float(self.pod_request_ram[service])

                total_cpu += temp_cpu
                total_ram += temp_ram

            # Iterate through available hosts
            for host in self.host_list:
                enough_resources = False

                if (total_cpu < float(self.current_node_available_cpu[host]) and total_ram < float(
                        self.current_node_available_ram[host])):
                    enough_resources = True

                # Check if resource demands are enough
                if enough_resources:
                    temp_tf = 0.0

                    # Calculate Traffic rates between services in current part of partition and services in current host
                    for service in self.app_partition[part]:
                        for x in self.current_placement[host]:
                            if service in self.current_placement[host] and x in self.current_placement[host]:
                                continue  # Same host
                            else:  # Different hosts
                                if service in self.service_affinities:
                                    if x in self.service_affinities[service]:
                                        temp_tf += float(self.service_affinities[service][x])
                                elif x in self.service_affinities:
                                    if service in self.service_affinities[x]:
                                        temp_tf += float(self.service_affinities[x][service])

                    # Calculate Most Loaded Situation - Prioritize CPU
                    temp_ml_cpu = float(self.current_node_usage_cpu[host]) + total_cpu
                    temp_ml_ram = float(self.current_node_usage_ram[host]) + total_ram

                    # Check Traffic Rates and Most-Loaded Situtations - Maximum searched
                    if (temp_tf > max_tf) or (temp_tf == max_tf and temp_ml_cpu > max_ml_cpu) or (
                            temp_tf == max_tf and temp_ml_cpu == max_ml_cpu and temp_ml_ram > max_ml_ram):
                        max_tf = temp_tf
                        max_ml_cpu = temp_ml_cpu
                        max_ml_ram = temp_ml_ram
                        max_host = host

            # Check max_host
            if max_host == '': # No host found -> return
                return {}
            else:
                # Update Resources
                self.current_node_available_cpu[max_host] = float(self.current_node_available_cpu[max_host]) - total_cpu
                self.current_node_available_ram[max_host] = float(self.current_node_available_ram[max_host]) - total_ram
                self.current_node_usage_cpu[max_host] = float(self.current_node_usage_cpu[max_host]) + total_cpu
                self.current_node_usage_ram[max_host] = float(self.current_node_usage_ram[max_host]) + total_ram

                # Update placement
                if max_host not in self.app_placement:
                    self.app_placement[max_host] = []

                # Append services in host
                for service in self.app_partition[part]:
                    self.app_placement[max_host].append(service)
