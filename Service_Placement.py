################################################
#                Cloud Service Placement for Optimizing Service Affinities
#                                  Author: Alkiviadis Aznavouridis
#                Undergraduate Student at Techinical Univercity of Crete
#                    Electronics and Computer Engineering Department
#          App: Online Boutique Shop implemented by Google Cloud Platform
#       URL: https://github.com/GoogleCloudPlatform/microservices-demo.git
################################################

import pandas as pd
import json
import numpy as np
import warnings
import time
import pprint
import sys
import re
import networkx as nx

from Application_Graph import Application_Graph
from Bin_Packing import Bin_Packing
from Bisecting_K_means import Bisecting_K_means
from GCP_Metrics import GCP_Metrics
from Binary_Partition import Binary_Partition
from Heuristic_First_Fit import Heuristic_First_Fit
from K_Partition import K_Partition

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)

# General Info of Placement for Google Cloud Platform - Kubernetes Cluster
kiali_port = 32002  # Kiali NodePort running on Kubernetes Cluster
prometheus_port = 32003  # Prometheus NodePort running on Kubernetes Cluster
namespace = "default"  # the namespace of the app


def construct_graph(service_list, service_affinities, service_to_id, id_to_service):
    G = nx.Graph()

    # Initialize Nodes
    for x in range(len(service_list)):
        G.add_node(x)
        service_to_id[service_list[x]] = x
        id_to_service[x] = service_list[x]

    # Insert Edges
    for source in service_affinities:
        for dest in service_affinities[source]:
            G.add_edge(service_to_id[source], service_to_id[dest], weight=float(service_affinities[source][dest]))

    return G


def print_menu():
    print("#" * 100)
    print("Pick an algorithm to produce the Service Placement")
    print("-" * 50)
    print("1) Heuristic First-Fit Algorithm")
    print("2) Binary Partition - Bin Packing")
    print("3) K-Partition - Bin Packing")
    print("4) Bisecting K-Means - Bin Packing")
    print("5) Exit")
    print("#" * 100)


def affinity_metric_menu():
    print("Pick the affinity metric")
    print("-" * 50)
    print("1) Requests per second")
    print("2) Mean value of bytes exchanged")
    print("#" * 100)


# Function to calculate total requested bytes before and after placement for measuring traffic between Egress
def calculate_total_bytes_requested(current_placement, final_placement, traffic_requested_bytes):
    initial_host_per_pod = {}
    final_host_per_pod = {}
    initial_bytes_requested = 0.0
    final_bytes_requested = 0.0

    # Find initial hosts per pod
    for host in current_placement:
        for service in current_placement[host]:
            initial_host_per_pod[service] = host

    # Find initial bytes requested
    for source in traffic_requested_bytes:
        for dest in traffic_requested_bytes[source]:
            # Check for same host
            if initial_host_per_pod[source] == initial_host_per_pod[dest]:
                continue
            else:
                initial_bytes_requested += float(traffic_requested_bytes[source][dest])

    # Find final hosts per pod
    for host in final_placement:
        for service in final_placement[host]:
            final_host_per_pod[service] = host

    # Find initial bytes requested
    for source in traffic_requested_bytes:
        for dest in traffic_requested_bytes[source]:
            # Check for same host
            if final_host_per_pod[source] == final_host_per_pod[dest]:
                continue
            else:
                final_bytes_requested += float(traffic_requested_bytes[source][dest])

    print("")
    print("#" * 100)
    print("Total traffic before and after placement in bytes")
    print("-" * 50)
    print("Initial Placement: " + str(initial_bytes_requested))
    print("Final Placement: " + str(final_bytes_requested))
    print("#" * 100)


def servicePlacement(host_ip):
    G = nx.Graph()
    # Initialize Class
    gcp_metrics_collector = GCP_Metrics(host_ip, kiali_port, prometheus_port, namespace)
    # Collect Resources from Prometheus and Prometheus
    gcp_metrics_collector.collect_resources()

    # Graph Constructor given the service list and affinities
    service_to_id = {}
    id_to_service = {}
    G = construct_graph(gcp_metrics_collector.service_list, gcp_metrics_collector.service_affinities, service_to_id,
                        id_to_service)

    # Print number of Hosts
    print("")
    print("#" * 100)
    print("Available Number of Hosts")
    print("-" * 40)
    pprint.pprint(len(gcp_metrics_collector.host_list))
    print("#" * 100)

    # Print Initial Placement
    print("")
    print("#" * 100)
    print("Initial Placement")
    print("-" * 40)
    pprint.pprint(gcp_metrics_collector.current_placement)
    print("#" * 100)

    # Show Response tmes for current placement
    print("")
    print("#" * 100)
    print("Latency between services in ms")
    print("-" * 40)
    pprint.pprint(gcp_metrics_collector.response_times)
    print("#" * 100)
    print("")

    # Choose between algorithms
    while True:
        print_menu()
        option = input('Pick an option:')
        if option.isnumeric():
            if 0 < int(option) < 5:
                while True:
                    affinity_metric_menu()
                    affinity_option = input('Pick an option:')
                    if affinity_option.isnumeric():
                        if 0 < int(affinity_option) < 3:
                            break
                    else:
                        print("Wrong Input! Pick a correct value!")
                break
        else:
            print("Wrong Input! Pick a correct value!")

    # Affinity Metric option
    if affinity_option == 1:
        affinity_metric = gcp_metrics_collector.service_affinities
    else:
        affinity_metric = gcp_metrics_collector.total_affinities_bytes

    # Algorithm choice
    if int(option) == 1:
        start_time = time.time()
        # Affinity Metric option
        if affinity_option == 1:
            affinity_metric = gcp_metrics_collector.affinities_collection
        else:
            affinity_metric = gcp_metrics_collector.affinities_bytes_collection

        # Heuristic Based Affinity Algorithm - A modified First-Fit algorithm
        heuristic_first_fit_algorithm = Heuristic_First_Fit(gcp_metrics_collector.current_placement,
                                                            gcp_metrics_collector.current_pod_request_cpu,
                                                            gcp_metrics_collector.current_pod_request_ram,
                                                            gcp_metrics_collector.node_available_cpu,
                                                            gcp_metrics_collector.node_available_ram,
                                                            affinity_metric,
                                                            gcp_metrics_collector.host_list)

        heuristic_first_fit_algorithm.heuristic_placement()
        end_time = time.time()
        placement_solution = heuristic_first_fit_algorithm.final_placement
        print("#" * 100)
        print("Heuristic First Fit placement")
        print("-" * 40)
        pprint.pprint(placement_solution)
        print("-" * 40)
        print("Execution time of algorithm:  --- %s seconds ---" % (end_time - start_time))
        print("#" * 100)
        heuristic_first_fit_algorithm.moved_services.clear()
    elif int(option) == 2:
        # Binary Partition - Bin Packing
        start_time = time.time()
        alpha = 1.0
        delta = 0.1
        placement_solution = {}

        while alpha >= 0.0:
            bp = Binary_Partition(gcp_metrics_collector.current_pod_request_cpu,
                                  gcp_metrics_collector.current_pod_request_ram,
                                  affinity_metric,
                                  gcp_metrics_collector.max_ram_allocation,
                                  gcp_metrics_collector.max_cpu_allocation,
                                  gcp_metrics_collector.host_list, gcp_metrics_collector.service_list)

            bp.calculate_app_partitions(alpha)
            # Insert Redis-Cart in Cart-Service partition
            for key in bp.app_partition:
                for x in range(len(bp.app_partition[key])):
                    if 'cartservice' == bp.app_partition[key][x]:
                        bp.app_partition[key].append('redis-cart')

            # Bin Packing
            bin_packing = Bin_Packing(bp.app_partition, gcp_metrics_collector.current_placement,
                                      gcp_metrics_collector.node_initial_cpu_usage,
                                      gcp_metrics_collector.node_initial_ram_usage,
                                      gcp_metrics_collector.node_initial_available_cpu,
                                      gcp_metrics_collector.node_initial_available_ram,
                                      gcp_metrics_collector.current_pod_request_cpu,
                                      gcp_metrics_collector.current_pod_request_ram,
                                      gcp_metrics_collector.host_list,
                                      affinity_metric)

            bin_packing.heuristic_packing()
            placement_solution = bin_packing.app_placement
            if bool(placement_solution):
                break
            else:
                alpha -= delta
        end_time = time.time()
        print("#" * 100)
        print("Binary Partition - Bin Packing Solution")
        print("-" * 40)
        pprint.pprint(placement_solution)
        print("-" * 40)
        print("Execution time of algorithm:  --- %s seconds ---" % (end_time - start_time))
        print("#" * 100)
    elif int(option) == 3:
        # K Partition - Bin Packing
        start_time = time.time()
        alpha = 1.0
        delta = 0.1
        placement_solution = {}

        while alpha >= 0.0:
            kp = K_Partition(gcp_metrics_collector.current_pod_request_cpu,
                             gcp_metrics_collector.current_pod_request_ram,
                             affinity_metric,
                             gcp_metrics_collector.max_ram_allocation,
                             gcp_metrics_collector.max_cpu_allocation,
                             gcp_metrics_collector.host_list, gcp_metrics_collector.service_list)

            kp.calculate_app_partitions(alpha)
            # Insert Redis-Cart in Cart-Service partition
            for key in kp.app_partition:
                for x in range(len(kp.app_partition[key])):
                    if 'cartservice' == kp.app_partition[key][x]:
                        kp.app_partition[key].append('redis-cart')

            # Bin Packing
            bin_packing = Bin_Packing(kp.app_partition, gcp_metrics_collector.current_placement,
                                      gcp_metrics_collector.node_initial_cpu_usage,
                                      gcp_metrics_collector.node_initial_ram_usage,
                                      gcp_metrics_collector.node_initial_available_cpu,
                                      gcp_metrics_collector.node_initial_available_ram,
                                      gcp_metrics_collector.current_pod_request_cpu,
                                      gcp_metrics_collector.current_pod_request_ram,
                                      gcp_metrics_collector.host_list,
                                      affinity_metric)

            bin_packing.heuristic_packing()
            placement_solution = bin_packing.app_placement
            if bool(placement_solution):
                break
            else:
                alpha -= delta
        end_time = time.time()
        print("#" * 100)
        print("K-Partition - Bin Packing Solution")
        print("-" * 40)
        pprint.pprint(placement_solution)
        print("-" * 40)
        print("Execution time of algorithm:  --- %s seconds ---" % (end_time - start_time))
        print("#" * 100)
    elif int(option) == 4:
        # Bisecting K-Means - Bin Packing

        # Insert K-Value
        while True:
            K_value = input('Choose value for K clusters to be created:')
            if K_value.isnumeric():
                if int(K_value) <= len(gcp_metrics_collector.service_list):
                    break
            else:
                print("Wrong Input! Given input is not an integer Value or greater than service list size!")

        start_time = time.time()
        bkm = Bisecting_K_means(affinity_metric, gcp_metrics_collector.service_list)
        bkm.find_bistecting_K_means_partitions(K_value)

        # Bin Packing
        bin_packing = Bin_Packing(bkm.app_clusters, gcp_metrics_collector.current_placement,
                                  gcp_metrics_collector.node_initial_cpu_usage,
                                  gcp_metrics_collector.node_initial_ram_usage,
                                  gcp_metrics_collector.node_initial_available_cpu,
                                  gcp_metrics_collector.node_initial_available_ram,
                                  gcp_metrics_collector.current_pod_request_cpu,
                                  gcp_metrics_collector.current_pod_request_ram,
                                  gcp_metrics_collector.host_list,
                                  affinity_metric)

        bin_packing.heuristic_packing()
        placement_solution = bin_packing.app_placement
        end_time = time.time()
        if not bool(placement_solution):
            print("ERROR: Placement solution hasn't been found!")
        else:
            print("#" * 100)
            print("Bisecting K-Means Clustering - Bin Packing Solution")
            print("-" * 40)
            pprint.pprint(placement_solution)
            print("-" * 40)
            print("Execution time of algorithm:  --- %s seconds ---" % (end_time - start_time))
            print("#" * 100)
    else:
        return

    # Calculate total requested bytes before and after placement
    calculate_total_bytes_requested(gcp_metrics_collector.current_placement, placement_solution,
                                    gcp_metrics_collector.traffic_requested_bytes)

    # Export initial Placement
    with open("final_markov_with_bin_packing_with_stressing.json", "w") as outfile:
        json.dump(gcp_metrics_collector.response_times, outfile)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("ERROR:External IP of one VM should be inserted as parameter! Try again!")
        exit(1)

    # Validate the formal of IP
    validate_IP = re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", sys.argv[1])
    if validate_IP:
        vm_external_ip = sys.argv[1]  # External ip for host machine to fetch the data (One required) given as input
        servicePlacement(vm_external_ip)
    else:
        print("ERROR:Wrong Format of External IP!Try again!")
        exit(1)
