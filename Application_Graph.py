######################################################
# Graph Construction Graph given the service list and the service affinities
# Input: Service List, Service Affinities
# Output: Networkx Graph
######################################################
import networkx as nx


class Application_Graph():
    def __init__(self, service_list, service_affinities):
        self.service_list = service_list
        self.service_affinities = service_affinities
        self.service_to_id = {}
        self.id_to_service = {}
        self.G = nx.Graph()

    # Initialize Nodes
    def find_graph_nodes(self):
        for x in range(len(self.service_list)):
            self.G.add_node(x)
            self.service_to_id[self.service_list[x]] = x
            self.id_to_service[x] = self.service_list[x]

    # Insert Edges
    def find_graph_edjes(self):
        for source in self.service_affinities:
            for dest in self.service_affinities[source]:
                self.G.add_edge(self.service_to_id[source], self.service_to_id[dest], weight=float(self.service_affinities[source][dest]))

    # Construct Graph
    def construct_graph(self):
        self.find_graph_nodes()
        self.find_graph_edjes()
        return self.G
