######################################################
# Heuristic Based Affinity Planner
# A modified First-Fit algorithm to produce a better service placement according to affinities and resources
# Input: Sorted Affinities, Resource demands and VM available resources
# Output: A new placement solution for current problem
# Paper:  Improving Microservice-Based application with runtime placement adaptation, Sampaio et al
######################################################
import copy


class Heuristic_First_Fit:
    def __init__(self, current_placement, pod_request_cpu, pod_request_ram, node_available_cpu, node_available_ram, service_affinities, host_list):
        self.final_placement = copy.deepcopy(current_placement)
        self.moved_services = []  # List to store which services have been moved (source-destination services)
        self.node_available_cpu = copy.deepcopy(node_available_cpu)
        self.node_available_ram = copy.deepcopy(node_available_ram)
        self.pod_request_cpu = copy.deepcopy(pod_request_cpu)
        self.pod_request_ram = copy.deepcopy(pod_request_ram)
        self.service_affinities = copy.deepcopy(service_affinities)
        self.host_list = host_list

    def heuristic_placement(self):
        for key in self.service_affinities:

            # Partition dictionary
            partition_key = key.partition('->')
            source_service = partition_key[0]
            dest_service = partition_key[2]

            # Initialize variables
            source_host = ""
            dest_host = ""
            dest_pod = ""
            source_pod = ""
            available_node_source_cpu = 0.0
            available_node_source_ram = 0.0
            available_node_dest_cpu = 0.0
            available_node_dest_ram = 0.0
            source_cpu = 0.0
            source_ram = 0.0
            dest_cpu = 0.0
            dest_ram = 0.0

            # Find resources
            for host in self.host_list:
                for service in self.final_placement[host]:

                    # Gather Resources of Source Service
                    if source_service == service:
                        source_cpu = float(self.pod_request_cpu[service])
                        source_ram = float(self.pod_request_ram[service])
                        available_node_source_cpu = float(self.node_available_cpu[host])
                        available_node_source_ram = float(self.node_available_ram[host])
                        source_host = host
                        source_pod = service

                    # Gather Resources of Destination Service
                    if dest_service == service:
                        dest_cpu = float(self.pod_request_cpu[service])
                        dest_ram = float(self.pod_request_ram[service])
                        available_node_dest_cpu = float(self.node_available_cpu[host])
                        available_node_dest_ram = float(self.node_available_ram[host])
                        dest_host = host
                        dest_pod = service

            # Check for same host
            if dest_host == source_host:
                # Mark them as moved so that they cant move again
                if dest_service not in self.moved_services:
                    self.moved_services.append(dest_service)
                if source_service not in self.moved_services:
                    self.moved_services.append(source_service)
                continue  # Proceed to next iteration
            else:
                moved_Flag = False
                # Check if destination service has already moved
                if dest_service not in self.moved_services:
                    if (dest_cpu < available_node_source_cpu) and (dest_ram < available_node_source_ram):
                        # CPU resources update
                        self.node_available_cpu[source_host] = float(self.node_available_cpu[source_host]) - dest_cpu
                        self.node_available_cpu[dest_host] = float(self.node_available_cpu[dest_host]) + dest_cpu

                        # RAM resources update
                        self.node_available_ram[source_host] = float(self.node_available_ram[source_host]) - dest_ram
                        self.node_available_ram[dest_host] = float(self.node_available_ram[dest_host]) + dest_ram

                        # Host services transfer and update
                        self.final_placement[dest_host].remove(dest_pod)
                        self.final_placement[source_host].append(dest_pod)
                        moved_Flag = True

                # Check if source service has already moved
                elif source_service not in self.moved_services:
                    if (source_cpu < available_node_dest_cpu) and (source_ram < available_node_dest_ram):
                        self.node_available_cpu[source_host] = float(self.node_available_cpu[source_host]) + source_cpu
                        self.node_available_cpu[dest_host] = float(self.node_available_cpu[dest_host]) - source_cpu

                        # RAM resources update
                        self.node_available_ram[source_host] = float(self.node_available_ram[source_host]) + source_ram
                        self.node_available_ram[dest_host] = float(self.node_available_ram[dest_host]) - source_ram

                        # Host services transfer and update
                        self.final_placement[source_host].remove(source_pod)
                        self.final_placement[dest_host].append(source_pod)
                        moved_Flag = True

            # If services "moved" then append them to list and continue
            if moved_Flag:
                if dest_service not in self.moved_services:
                    self.moved_services.append(dest_service)

                if source_service not in self.moved_services:
                    self.moved_services.append(source_service)

