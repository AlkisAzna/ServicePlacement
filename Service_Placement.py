################################################
#                Cloud Service Placement for Optimizing Service Affinities
#                                  Author: Alkiviadis Aznavouridis
#                Undergraduate Student at Techinical Univercity of Crete
#                    Electronics and Computer Engineering Department
#          App: Online Boutique Shop implemented by Google Cloud Platform
#       URL: https://github.com/GoogleCloudPlatform/microservices-demo.git
################################################

import pandas as pd
import numpy as np
import warnings
import requests
import pprint
import operator
import copy
import sys
import re
import networkx as nx

from Application_Graph import Application_Graph
from Bin_Packing import Bin_Packing
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
        print("#" * 100)
        print("Heuristic First Fit placement")
        print("-" * 40)
        pprint.pprint(heuristic_first_fit_algorithm.final_placement)
        print("#" * 100)
        heuristic_first_fit_algorithm.moved_services.clear()
    elif int(option) == 2:
        # Binary Partition - Bin Packing
        alpha = 1.0
        delta = 0.1
        placement_solution = {}

        while alpha >= 0.0:
            bp = Binary_Partition(gcp_metrics_collector.current_pod_request_cpu,
                                  gcp_metrics_collector.current_pod_request_ram,
                                  gcp_metrics_collector.service_affinities,
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
                                      gcp_metrics_collector.service_affinities)

            bin_packing.heuristic_packing()
            placement_solution = bin_packing.app_placement
            if bool(placement_solution):
                break
            else:
                alpha -= delta

        print("#" * 100)
        print("Binary Partition - Bin Packing Solution")
        print("-" * 40)
        pprint.pprint(placement_solution)
        print("#" * 100)
    elif int(option) == 3:
        # K Partition - Bin Packing
        alpha = 1.0
        delta = 0.1
        placement_solution = {}

        while alpha >= 0.0:
            kp = K_Partition(gcp_metrics_collector.current_pod_request_cpu,
                             gcp_metrics_collector.current_pod_request_ram,
                             gcp_metrics_collector.service_affinities,
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
                                      gcp_metrics_collector.service_affinities)

            bin_packing.heuristic_packing()
            placement_solution = bin_packing.app_placement
            if bool(placement_solution):
                break
            else:
                alpha -= delta

        print("#" * 100)
        print("Binary Partition - Bin Packing Solution")
        print("-" * 40)
        pprint.pprint(placement_solution)
        print("#" * 100)
    elif int(option) == 4:
        # Binary Partition - Bin Packing
        alpha = 1.0
        delta = 0.1
        placement_solution = {}

        while alpha >= 0.0:
            kp = K_Partition(gcp_metrics_collector.current_pod_request_cpu,
                             gcp_metrics_collector.current_pod_request_ram,
                             gcp_metrics_collector.service_affinities,
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
                                      gcp_metrics_collector.service_affinities)

            bin_packing.heuristic_packing()
            placement_solution = bin_packing.app_placement
            if bool(placement_solution):
                break
            else:
                alpha -= delta

        print("#" * 100)
        print("Binary Partition - Bin Packing Solution")
        print("-" * 40)
        pprint.pprint(placement_solution)
        print("#" * 100)
    else:
        return


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
