######################################################
# Class for collection Kubernetes Cluster Metrics though Kiali and Prometheus through APIs
######################################################
import copy
import json
import pprint
import re
import requests
import operator


class GCP_Metrics:
    def __init__(self, vm_external_ip, kiali_port, prometheus_port, namespace):
        self.url_prometheus = "http://" + vm_external_ip + ":" + str(prometheus_port) + "/api/v1/query"
        self.url_kiali = "http://" + vm_external_ip + ":" + str(kiali_port) + "/kiali/api/namespaces/graph"
        self.namespace = namespace
        self.host_list = []
        self.service_list = []
        self.number_of_hosts = 0

        # Node Data
        self.node_request_cpu = {}
        self.node_request_ram = {}
        self.node_allocatable_cpu = {}
        self.node_allocatable_ram = {}
        self.node_available_cpu = {}
        self.node_available_ram = {}
        self.node_initial_available_cpu = {}
        self.node_initial_available_ram = {}
        self.node_initial_cpu_usage = {}
        self.node_initial_ram_usage = {}
        self.max_cpu_allocation = 0.0
        self.max_ram_allocation = 0.0

        # Pod Data
        self.pod_request_cpu = {}  # Pod names are as described in Kubernetes Cluster
        self.pod_request_ram = {}
        self.current_pod_request_cpu = {}  # Pod name is only the app service
        self.current_pod_request_ram = {}
        self.pod_usage_cpu = {}
        self.pod_usage_ram = {}

        # Traffic measurements
        self.traffic_requested_bytes = {}
        self.traffic_responsed_bytes = {}
        self.traffic_requested_count = {}
        self.traffic_responsed_count = {}

        # Service Affinities
        self.total_edjes = 0
        self.response_times = {}
        # Affinities by Request Per Second
        self.service_affinities = {}  # Pattern: {"source1": {"dest1": 'Req_Per_Sec_Value', "dest2": value2, ...}, "source2" : { ...}}
        self.sorted_service_affinities = {}  # Same Pattern as service affinities but sorted for each source
        self.affinities_collection = {}  # Pattern: {"source->dest": Req_Per_Sec_Value, ....}
        # Affinities by Bytes exchanged
        self.total_affinities_bytes = {}
        self.affinities_bytes_collection = {}

        # Placement
        self.initial_placement = {}
        self.current_placement = {}

    # Functions to collect Kubernetes Cluster Metrics and save them to be processed
    # Collect Requests from Nodes
    def kube_node_requests(self):
        # Headers of cURL command
        headers_prometheus = {'cache-control': "no-cache"}
        # CPU request
        app_request = {"query": "sum(kube_pod_container_resource_requests_cpu_cores) by (node)"}
        # cURL command for Node Ram Usage
        response = requests.request("GET", self.url_prometheus, headers=headers_prometheus, params=app_request)
        result = json.loads(response.text)
        for x in result['data']['result']:
            if bool(x['metric']):
                self.host_list.append(x['metric']['node'])
                self.node_request_cpu[x['metric']['node']] = format(float(x['value'][1]), '.3f')

        self.number_of_hosts = len(self.host_list)
        self.max_cpu_allocation = format(float(max(self.node_request_cpu.values())), '.3f')

        # RAM Requests
        app_request = {"query": "sum(kube_pod_container_resource_requests_memory_bytes) by (node)"}
        # cURL command for Node Ram Usage
        response = requests.request("GET", self.url_prometheus, headers=headers_prometheus, params=app_request)
        result = json.loads(response.text)
        for node in result['data']['result']:
            if bool(node['metric']):
                self.node_request_ram[node['metric']['node']] = format(float(node['value'][1]), '.3f')
        self.max_ram_allocation = format(float(max(self.node_request_ram.values())), '.3f')

        # CPU allocation
        app_request = {"query": "kube_node_status_allocatable{resource='cpu'}"}
        # cURL command for Node Ram Usage
        response = requests.request("GET", self.url_prometheus, headers=headers_prometheus, params=app_request)
        result = json.loads(response.text)
        for node in result['data']['result']:
            self.node_allocatable_cpu[node['metric']['node']] = node['value'][1]

        # RAM allocation
        app_request = {"query": "kube_node_status_allocatable{resource='memory'}"}
        # cURL command for Node Ram Usage
        response = requests.request("GET", self.url_prometheus, headers=headers_prometheus, params=app_request)
        result = json.loads(response.text)
        for node in result['data']['result']:
            self.node_allocatable_ram[node['metric']['node']] = node['value'][1]

        # Available Node CPU resources (Allocatable-Requested)
        for host in self.node_request_cpu:
            self.node_available_cpu[host] = float(self.node_allocatable_cpu[host]) - float(self.node_request_cpu[host])
            self.node_available_ram[host] = float(self.node_allocatable_ram[host]) - float(self.node_request_ram[host])

    # Collect Requests from Pods
    def kube_pod_requests(self):
        # Headers of cURL command
        headers_prometheus = {'cache-control': "no-cache"}
        # CPU request
        app_request = {
            "query": "sum(kube_pod_container_resource_requests_cpu_cores{namespace='" + self.namespace + "'}) by (pod)"}
        # cURL command for Node Ram Usage
        response = requests.request("GET", self.url_prometheus, headers=headers_prometheus, params=app_request)
        result = json.loads(response.text)
        for service in result['data']['result']:
            self.pod_request_cpu[service['metric']['pod']] = service['value'][1]

        # RAM Requests
        app_request = {
            "query": "sum(kube_pod_container_resource_requests_memory_bytes{namespace='" + self.namespace + "'}) by (pod)"}
        # cURL command for Node Ram Usage
        response = requests.request("GET", self.url_prometheus, headers=headers_prometheus, params=app_request)
        result = json.loads(response.text)

        for service in result['data']['result']:
            self.pod_request_ram[service['metric']['pod']] = service['value'][1]

    # Collect the pod average usage resources
    def kube_pod_usage_resources(self):
        # Headers of cURL command
        headers_prometheus = {'cache-control': "no-cache"}

        # Ram Usage per Pod
        for i in range(self.number_of_hosts):
            query_pod_ram = {"query": "avg(container_memory_max_usage_bytes{instance='" + self.host_list[
                i] + "', namespace='" + self.namespace + "', pod!~'billowing.*'}) by (pod)"}
            self.pod_usage_ram[self.host_list[i]] = {}

            # cURL command for Pod Ram Usage
            response = requests.request("GET", self.url_prometheus, headers=headers_prometheus, params=query_pod_ram)
            result = json.loads(response.text)

            number_of_pods = len(result["data"]["result"])
            for k in range(number_of_pods):
                pod = result["data"]["result"][k]["metric"]["pod"]
                self.pod_usage_ram[self.host_list[i]][pod] = format(float(result["data"]["result"][k]["value"][1]),
                                                                    '.2f')

        # CPU Usage per Pod
        for i in range(self.number_of_hosts):
            query_pod_cpu = {"query": "avg(rate(container_cpu_usage_seconds_total{kubernetes_io_hostname='" + str(
                self.host_list[i]) + "',pod!~'billowing.*', namespace='" + self.namespace + "'}[30m])) by (pod)"}
            self.pod_usage_cpu[self.host_list[i]] = {}

            # cURL command for Pod Cpu Usage
            response = requests.request("GET", self.url_prometheus, headers=headers_prometheus, params=query_pod_cpu)
            result = json.loads(response.text)

            self.initial_placement[self.host_list[i]] = []
            serv_list = []
            number_of_pods = len(result["data"]["result"])
            for k in range(number_of_pods):
                serv_list.append(result["data"]["result"][k]["metric"]["pod"])
                self.initial_placement[self.host_list[i]].append(serv_list[k])
                self.pod_usage_cpu[self.host_list[i]][serv_list[k]] = format(
                    float(result["data"]["result"][k]["value"][1]), '.4f')
            # Save the services with their app name
            for x in range(len(serv_list)):
                split_string = re.split("-", serv_list[x])
                if len(split_string) == 3:
                    curr_service = split_string[0]
                else:
                    if split_string[1] == 'cart':
                        curr_service = split_string[0] + '-' + split_string[1]
                    else:
                        curr_service = split_string[0]
                self.service_list.append(curr_service)
            serv_list.clear()

    # Collect the service affinities and response times from kiali
    def kube_service_affinities(self):
        headers_kiali = {'cache-control': "no-cache"}
        query_string_kiali = {"duration": "30m", "namespaces": self.namespace,
                              "graphType": "workload"}  # Graph type must be Wokload and Duration is set as required

        # cURL command
        response = requests.request("GET", self.url_kiali, headers=headers_kiali, params=query_string_kiali)
        result = json.loads(response.text)

        # INFO NOTE: redis-cart won't appear from kiali graph. There must be internal communication between cartservice and redis-cart so these two pods should be placed together and calculate as one
        # Graph Services ID

        services_id = {}
        unused_services_id = {}  # Unknown or unused services in graph should be omitted
        for i in range(len(result["elements"]["nodes"])):
            if result["elements"]["nodes"][i]["data"]["namespace"] == self.namespace:
                if "app" not in result["elements"]["nodes"][i]["data"] or "traffic" not in \
                        result["elements"]["nodes"][i]["data"]:
                    if "app" in result["elements"]["nodes"][i]["data"]:
                        key = result["elements"]["nodes"][i]["data"]["id"]
                        unused_services_id[key] = result["elements"]["nodes"][i]["data"]["app"]
                        continue
                    key = result["elements"]["nodes"][i]["data"]["id"]
                    unused_services_id[key] = result["elements"]["nodes"][i]["data"]["service"]
                    continue
                key = result["elements"]["nodes"][i]["data"]["id"]
                services_id[key] = result["elements"]["nodes"][i]["data"]["app"]

        # Graph edges - Affinities
        self.total_edjes = len(result["elements"]["edges"])
        for i in range(self.total_edjes):
            source_id = result["elements"]["edges"][i]["data"]["source"]  # Source ID
            destination_id = result["elements"]["edges"][i]["data"]["target"]  # Destination ID
            # Avoid traces from unused services dictionary
            if (source_id in unused_services_id.keys()) or (destination_id in unused_services_id.keys()):
                continue

            # Track all traces in service id
            if (source_id in services_id.keys()) and (destination_id in services_id.keys()):
                if services_id[source_id] not in self.service_affinities.keys():
                    self.service_affinities[services_id[source_id]] = {}
                    self.response_times[services_id[source_id]] = {}
                protocol = result["elements"]["edges"][i]["data"]["traffic"]["protocol"]  # Protocol of communication
                self.service_affinities[services_id[source_id]][services_id[destination_id]] = \
                    result["elements"]["edges"][i]["data"]["traffic"]["rates"][protocol]
                try:
                    self.response_times[services_id[source_id]][services_id[destination_id]] = \
                        result["elements"]["edges"][i]["data"]["responseTime"]
                except:
                    continue

        # Sort Affinities
        self.sorted_service_affinities = copy.deepcopy(self.service_affinities)
        for key in self.service_affinities:
            self.sorted_service_affinities[key] = dict(
                sorted(self.sorted_service_affinities[key].items(), key=operator.itemgetter(1), reverse=True))

        # Assemble all affinities in one matrix in decent order
        sorted_dict = {}
        for source_key in self.sorted_service_affinities:
            for destination_key in self.sorted_service_affinities[source_key]:
                sorted_dict[source_key + "->" + destination_key] = float(
                    self.sorted_service_affinities[source_key][destination_key])
        self.affinities_collection = dict(sorted(sorted_dict.items(), key=operator.itemgetter(1), reverse=True))

    # Collect total requested and responsed bytes for each service
    def kube_total_affinity_bytes(self):
        # Headers of cURL command
        headers_prometheus = {'cache-control': "no-cache"}
        request_query = {"query": "istio_request_bytes_sum{response_code = '" + str(
            200) + "', connection_security_policy = 'mutual_tls', source_app != 'unknown',  destination_app != 'unknown'}"}
        response_query = {"query": "istio_response_bytes_sum{response_code = '" + str(
            200) + "', connection_security_policy = 'mutual_tls', source_app != 'unknown',  destination_app != 'unknown'}"}
        req_count_query = {"query": "istio_request_bytes_count{response_code = '" + str(
            200) + "', connection_security_policy = 'mutual_tls', source_app != 'unknown',  destination_app != 'unknown'}"}
        resp_count_query = {"query": "istio_response_bytes_count{response_code = '" + str(
            200) + "', connection_security_policy = 'mutual_tls', source_app != 'unknown',  destination_app != 'unknown'}"}

        # cURL command for Request Metrics
        response = requests.request("GET", self.url_prometheus, headers=headers_prometheus, params=request_query)
        req_result = json.loads(response.text)
        response = requests.request("GET", self.url_prometheus, headers=headers_prometheus, params=response_query)
        resp_result = json.loads(response.text)
        response = requests.request("GET", self.url_prometheus, headers=headers_prometheus, params=req_count_query)
        req_count_result = json.loads(response.text)
        response = requests.request("GET", self.url_prometheus, headers=headers_prometheus, params=resp_count_query)
        resp_count_result = json.loads(response.text)

        total_queries = len(req_result['data']['result'])
        # Iterate throught Results
        for x in range(total_queries):
            source_app = req_result['data']['result'][x]['metric']['source_app']
            if source_app not in self.total_affinities_bytes:
                self.traffic_requested_bytes[source_app] = {}
                self.traffic_responsed_bytes[source_app] = {}
                self.traffic_requested_count[source_app] = {}
                self.traffic_responsed_count[source_app] = {}
                self.total_affinities_bytes[source_app] = {}
            dest_app = req_result['data']['result'][x]['metric']['destination_app']
            self.traffic_requested_bytes[source_app][dest_app] = float(req_result['data']['result'][x]['value'][1])
            self.traffic_responsed_bytes[source_app][dest_app] = float(resp_result['data']['result'][x]['value'][1])
            self.traffic_requested_count[source_app][dest_app] = float(
                req_count_result['data']['result'][x]['value'][1])
            self.traffic_responsed_count[source_app][dest_app] = float(
                resp_count_result['data']['result'][x]['value'][1])
            self.total_affinities_bytes[source_app][dest_app] = format(float((float(
                req_result['data']['result'][x]['value'][1]) + float(resp_result['data']['result'][x]['value'][1])) / (
                                                                                     float(req_count_result['data'][
                                                                                               'result'][x][
                                                                                               'value'][1]) + float(
                                                                                 resp_count_result['data'][
                                                                                     'result'][x]['value'][1]))),
                                                                       '.3f')

        # Assemble all affinities in one matrix in decent order
        for source_key in self.total_affinities_bytes:
            for destination_key in self.total_affinities_bytes[source_key]:
                self.affinities_bytes_collection[source_key + "->" + destination_key] = float(
                    self.total_affinities_bytes[source_key][destination_key])
                self.affinities_bytes_collection = dict(
                    sorted(self.affinities_bytes_collection.items(), key=operator.itemgetter(1), reverse=True))

    # Adjust service names in Initial Placement
    def refactor_placement(self):
        for key in self.initial_placement:
            self.current_placement[key] = []
            for index, services in enumerate(self.initial_placement[key]):
                # Pattern: service_name-ID-SubID
                split_string = re.split("-", services)
                if len(split_string) == 3:
                    curr_service = split_string[0]
                else:
                    if split_string[1] == 'cart':
                        curr_service = split_string[0] + '-' + split_string[1]
                    else:
                        curr_service = split_string[0]
                self.current_placement[key].append(curr_service)

    # Adjust service name in Pod Requests Dictionaries
    def refactor_pod_requests(self):
        for services in self.pod_request_cpu.keys():
            # Pattern: service_name-ID-SubID
            split_string = re.split("-", services)
            if len(split_string) == 3:
                curr_service = split_string[0]
            else:
                if split_string[1] == 'cart':
                    curr_service = split_string[0] + '-' + split_string[1]
                else:
                    curr_service = split_string[0]

            self.current_pod_request_cpu[curr_service] = format(float(self.pod_request_cpu[services]), '.3f')
            self.current_pod_request_ram[curr_service] = format(float(self.pod_request_ram[services]), '.3f')

    # Function to find the available and used space for each VM without the current pods
    def calculate_node_metrics_without_pods(self):
        self.node_initial_available_cpu = copy.deepcopy(self.node_available_cpu)
        self.node_initial_available_ram = copy.deepcopy(self.node_available_ram)
        self.node_initial_cpu_usage = copy.deepcopy(self.node_request_cpu)
        self.node_initial_ram_usage = copy.deepcopy(self.node_request_ram)

        for host in self.host_list:
            if bool(self.current_placement[host]):
                total_requested_cpu = 0.0
                total_requested_ram = 0.0
                for service in self.current_placement[host]:
                    total_requested_cpu += float(self.current_pod_request_cpu[service])
                    total_requested_ram += float(self.current_pod_request_ram[service])

                self.node_initial_available_cpu[host] = float(
                    self.node_initial_available_cpu[host]) + total_requested_cpu
                self.node_initial_available_ram[host] = float(
                    self.node_initial_available_ram[host]) + total_requested_ram
                self.node_initial_cpu_usage[host] = float(self.node_initial_cpu_usage[host]) - total_requested_cpu
                self.node_initial_ram_usage[host] = float(self.node_initial_ram_usage[host]) - total_requested_ram

    # Function to collect data and initiliaze class variables
    def collect_resources(self):
        self.kube_node_requests()
        self.kube_pod_requests()
        self.kube_pod_usage_resources()
        self.kube_service_affinities()
        self.kube_total_affinity_bytes()
        self.refactor_placement()
        self.refactor_pod_requests()
        self.calculate_node_metrics_without_pods()
