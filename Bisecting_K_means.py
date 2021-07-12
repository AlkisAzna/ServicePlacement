######################################################
# A modified version of Bisecting K-Means algorithm to produce app clusters given the K value of Cluster
# Input: Service List, Service Affinities, K value
# Output: K clusters with high affinity
######################################################
import copy
import random
import sys


class Bisecting_K_means:
    def __init__(self, service_affinities, service_list):
        self.service_affinities = copy.deepcopy(service_affinities)
        self.service_list = service_list
        self.total_nodes = len(service_list)
        self.app_clusters = {}

    def find_bistecting_K_means_partitions(self):
        # Insert K-Value
        while True:
            K_value = input('Choose value for K clusters to be created:')
            if K_value.isnumeric():
                if int(K_value) <= self.total_nodes:
                    break
            else:
                print("Wrong Input! Given input is not an integer Value or greater than service list size!")

        parent_cluster = copy.deepcopy(self.service_list)
        self.app_clusters = {"1": parent_cluster}
        cluster_affinities = {"1": 0.0}
        cluster_count = 1
        index = -1
        last_index = 1

        while cluster_count < int(K_value):
            # Find the cluster with the least sum of affinities - Maximum Error
            min_total_affinity = sys.float_info.max
            for x in self.app_clusters:
                # If cluster contains only one service skip
                if len(self.app_clusters[x]) == 1:
                    continue
                if cluster_affinities[x] < min_total_affinity:
                    parent_cluster = self.app_clusters[x]
                    index = x

            # Remove the cluster to be split up
            self.app_clusters.pop(index)
            cluster_affinities.pop(index)

            # Pick centroids according to less or no affinities and remove them from list
            if len(parent_cluster) == 2:
                # Cluster contains only 2 services - > Make them centroids
                first_centroid = random.choice(parent_cluster)
                parent_cluster.remove(first_centroid)
                second_centroid = random.choice(parent_cluster)
                parent_cluster.remove(second_centroid)
            else:
                centroids_found = False
                min_affinity = sys.float_info.max
                first_centroid = ""
                second_centroid = ""
                # Cluster contains more than 2 clusters -> Find min or no affinity and pick the centroids accordingly
                for first_service in parent_cluster:
                    for second_service in parent_cluster:
                        if first_service == second_service:
                            # Same service
                            continue
                        else:
                            # Check for affinity
                            if first_service in self.service_affinities:
                                if second_service in self.service_affinities[first_service]:
                                    if min_affinity > float(self.service_affinities[first_service][second_service]):
                                        min_affinity = float(self.service_affinities[first_service][second_service])
                                        first_centroid = first_service
                                        second_centroid = second_service
                                else:
                                    # They dont have an affinity so pick them for centroids
                                    centroids_found = True
                                    first_centroid = first_service
                                    second_centroid = second_service
                                    break
                            elif second_service in self.service_affinities:
                                if first_service in self.service_affinities[second_service]:
                                    if min_affinity > float(self.service_affinities[second_service][first_service]):
                                        min_affinity = float(self.service_affinities[second_service][first_service])
                                        first_centroid = first_service
                                        second_centroid = second_service
                                else:
                                    # They dont have an affinity so pick them for centroids
                                    centroids_found = True
                                    first_centroid = first_service
                                    second_centroid = second_service
                                    break
                            else:
                                # They dont have an affinity so pick them for centroids
                                centroids_found = True
                                first_centroid = first_service
                                second_centroid = second_service
                                break

                                # Check if centroids have been found
                    if centroids_found:
                        break

                parent_cluster.remove(first_centroid)
                parent_cluster.remove(second_centroid)

            # Create Lists for centroids
            self.app_clusters[last_index] = [first_centroid]
            self.app_clusters[last_index + 1] = [second_centroid]

            sum_affinity_centroid_1 = 0.0
            sum_affinity_centroid_2 = 0.0
            # Insert services to the random generated centroids
            while parent_cluster:
                curr_service = parent_cluster.pop(len(parent_cluster) - 1)
                affinity_centroid_1 = 0.0
                affinity_centroid_2 = 0.0

                # Check if service belongs to keys of service_affinities dictionary
                if curr_service in self.service_affinities:
                    # Check the affinities with centroids
                    if first_centroid in self.service_affinities[curr_service]:
                        affinity_centroid_1 += float(self.service_affinities[curr_service][first_centroid])
                    elif second_centroid in self.service_affinities[curr_service]:
                        affinity_centroid_2 += float(self.service_affinities[curr_service][second_centroid])

                # Check if centroids contain the current service
                if first_centroid in self.service_affinities:
                    if curr_service in self.service_affinities[first_centroid]:
                        affinity_centroid_1 += float(self.service_affinities[first_centroid][curr_service])

                if second_centroid in self.service_affinities:
                    if curr_service in self.service_affinities[second_centroid]:
                        affinity_centroid_2 += float(self.service_affinities[second_centroid][curr_service])

                # Assign service the best centroid according to max affinity
                sum_affinity_centroid_1 += affinity_centroid_1
                sum_affinity_centroid_2 += affinity_centroid_2
                if affinity_centroid_1 < sum_affinity_centroid_2:
                    self.app_clusters[last_index + 1].append(curr_service)
                elif affinity_centroid_1 > sum_affinity_centroid_2:
                    self.app_clusters[last_index].append(curr_service)
                else:
                    self.app_clusters[random.choice([last_index, last_index + 1])].append(curr_service)

            # Update total affinities
            cluster_affinities[last_index] = sum_affinity_centroid_1
            cluster_affinities[last_index + 1] = sum_affinity_centroid_2

            # Update variables
            last_index += 2
            cluster_count += 1
