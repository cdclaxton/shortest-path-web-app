# This script generates a large dataset for stress-testing the ingest capability of the web-app.

import json
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import os
import random
from scipy import stats


def make_connected_component(num_nodes):
    """Make an adjacency list for a single connected component."""

    # Preconditions
    assert type(num_nodes) == int
    assert num_nodes > 0, "Number of nodes must be positive"

    # Initialise the connections
    connections = {i: [] for i in range(num_nodes)}

    # Node 0 acts as the seed node
    for node in range(1, num_nodes):

        min_node = 0
        max_node = node-1

        dst = random.randint(min_node, max_node)

        connections[node].append(dst)

    return connections


def draw_network(adj):
    """Draw the graph given its adjacency list."""

    # Preconditions
    assert type(adj) == dict

    graph = nx.Graph()

    for src, dsts in adj.items():
        for dst in dsts:
            graph.add_edge(src, dst)

    nx.draw(graph)
    plt.show()


def make_entity_id(node_id):
    """Make the entity ID given a node's ID."""

    # Preconditions
    assert type(node_id) == int
    return f"e-{node_id}"


def make_document_id(doc_index):
    """Make the document ID given the document's index."""

    # Preconditions
    assert doc_index >= 0
    return f"d-{doc_index}"


def assign_entity_ids(adj, offset):
    """Assign entity IDs to each node in the graph."""

    # Preconditions
    assert type(adj) == dict
    assert offset >= 0

    adj_assigned = dict()
    max_node_id = -1

    for src, dsts in adj.items():

        max_node_id = max(src, max_node_id)

        for d in dsts:
            max_node_id = max(d, max_node_id)

        adj_assigned[make_entity_id(
            src + offset)] = [make_entity_id(d + offset) for d in dsts]

    return adj_assigned, max_node_id+1


def insert_documents(adj, doc_id_offset):
    """Insert documents between entities to form a bipartite graph."""

    # Preconditions
    assert type(adj) == dict
    assert doc_id_offset >= 0

    current_doc_index = doc_id_offset

    links = []

    for src, dsts in adj.items():
        for d in dsts:

            # Number of documents between two entities is sampled from a distribution
            num_docs = np.argmax(np.random.multinomial(
                1, [0, 0.3, 0.3, 0.2, 0.1, 0.1]) > 0)
            assert num_docs > 0

            for _ in range(num_docs):
                doc_id = make_document_id(current_doc_index)
                current_doc_index += 1

                # Add the entity -- document -- entity link
                links.append((src, doc_id))
                links.append((d, doc_id))

    return links, current_doc_index


def make_entities(adj):
    """Make simple entities."""

    # Preconditions
    assert type(adj) == dict

    # Get the entity IDs
    entity_ids = set()
    for src, dsts in adj.items():
        entity_ids.add(src)
        for d in dsts:
            entity_ids.add(d)

    # Return a list of tuples of (entity ID, entity label)
    return [(entity_id, f"Entity {entity_id}") for entity_id in entity_ids]


def make_documents(links):
    """Make simple documents."""

    # Preconditions
    assert type(links) == list

    # Get the unique document IDs
    document_ids = set()
    for link in links:
        _, document_id = link
        document_ids.add(document_id)

    # Return a list of tuples of (document ID, document label, document date)
    return [(doc_id, f"Document {doc_id}", "02/01/2023") for doc_id in document_ids]


class BatchWriter:
    """Write CSV data across one or more files."""

    def __init__(self, folder, file_prefix, num_files, header):

        # Preconditions
        assert os.path.exists(
            folder), f"Folder doesn't exist: {folder}"
        assert type(file_prefix) == str
        assert num_files > 0
        assert type(header) == list
        assert len(header) > 0

        # Store parameters
        self.folder = folder
        self.header = header
        self.num_files = num_files

        # Index of the next file to write to
        self.file_idx_next_row = 0

        # Open the files and add the header
        self.fps = []
        for i in range(0, self.num_files):
            filepath = os.path.join(self.folder, f"{file_prefix}-{i}.csv")
            fp = open(filepath, 'w')
            fp.write(self._make_row(header))
            self.fps.append(fp)

    def _make_row(self, row):
        return ",".join(row) + "\n"

    def add_entry(self, entry):
        """Add a row to a file."""

        assert type(entry) == tuple
        assert len(entry) == len(self.header)

        # Write the row to the file
        fp = self.fps[self.file_idx_next_row]
        fp.write(self._make_row(entry))

        self.file_idx_next_row = (self.file_idx_next_row + 1) % self.num_files

    def add_entries(self, entries):
        """Add multiple rows."""

        assert type(entries) == list

        for e in entries:
            self.add_entry(e)

    def close(self):
        """Close all of the files."""

        for fp in self.fps:
            fp.close()


def build_config(num_entities_files, num_documents_files, num_links_files):
    """Build config in the required form for the web-app."""

    # Preconditions
    assert type(num_entities_files) == int
    assert num_entities_files > 0

    assert type(num_documents_files) == int
    assert num_documents_files > 0

    assert type(num_links_files) == int
    assert num_links_files > 0

    entities_config = [{
        "path": f"entities-{i}.csv",
        "entityType": "Person",
        "delimiter": ",",
        "entityIdField": "ID",
        "fieldToAttribute": {
            "label": "label"
        }
    } for i in range(num_entities_files)]

    documents_config = [{
        "path": f"documents-{i}.csv",
        "documentType": "Type-A",
        "delimiter": ",",
        "documentIdField": "ID",
        "fieldToAttribute": {
            "label": "label",
            "date": "date"
        }
    } for i in range(num_documents_files)]

    links_config = [{
        "path": f"links-{i}.csv",
        "entityIdField": "entity ID",
        "documentIdField": "document ID",
        "delimiter": ","
    } for i in range(num_links_files)]

    bipartite_graph_config = {
        "type": "pebble",
        "folder": "<TEMP>",
        "deleteFilesInFolder": True
    }

    unipartite_graph_config = {
        "type": "pebble",
        "folder": "<TEMP>",
        "deleteFilesInFolder": True
    }

    return {
        "graphData": {
            "entitiesFiles": entities_config,
            "documentsFiles": documents_config,
            "linksFiles": links_config,
            "skipEntitiesFile": "skip_entities.txt"
        },
        "bipartiteGraphConfig": bipartite_graph_config,
        "unipartiteGraphConfig": unipartite_graph_config
    }


def write_config_json(output_folder, config):
    """Write JSON config to file."""

    assert type(output_folder) == str
    assert os.path.exists(
        output_folder), f"Folder doesn't exist: {output_folder}"
    assert type(config) == dict

    filepath = os.path.join(output_folder, "data-config.json")
    with open(filepath, 'w') as fp:
        fp.write(json.dumps(config, indent=4))


if __name__ == '__main__':

    # Location in which to save the generated data
    output_folder = "test-data-sets/set-3"

    # Number of each type of file to create
    num_documents_files = 2
    num_entities_files = 2
    num_links_files = 2

    num_connected_components = 2

    exp_scale = 500
    x = np.arange(0, 20000, 10)
    y = [stats.expon.pdf(xi, 0, exp_scale) for xi in x]

    plt.plot(x, y)
    plt.xlabel('Number of nodes in a connected component')
    plt.ylabel('Probability')
    plt.show()

    # Make the data folder
    data_folder = os.path.join(output_folder, "data")
    if not os.path.exists(data_folder):
        os.mkdir(data_folder)

    # Batch CSV writers
    documents_writer = BatchWriter(
        data_folder, "documents", num_documents_files, ["ID", "label", "date"])

    entities_writer = BatchWriter(
        data_folder, "entities", num_entities_files, ["ID", "label"])

    links_writer = BatchWriter(
        data_folder, "links", num_links_files, ["entity ID", "document ID"])

    next_entity_offset = 0
    next_doc_offset = 0

    for _ in range(num_connected_components):

        # Sample the number of entities in the connected component
        num_entities = int(stats.expon.rvs(0, exp_scale))
        assert num_entities > 0

        # Make the connected component consisting of entities only
        adj = make_connected_component(num_entities)

        # Assign entity IDs to the nodes
        adj_assigned, next_entity_offset = assign_entity_ids(
            adj, next_entity_offset)

        # Insert documents between entity links
        links, next_doc_offset = insert_documents(
            adj_assigned, next_doc_offset)

        # Write entities to file
        entity_csv_data = make_entities(adj_assigned)
        entities_writer.add_entries(entity_csv_data)

        # Write documents to file
        documents_csv_data = make_documents(links)
        documents_writer.add_entries(documents_csv_data)

        # Write links to file
        links_writer.add_entries(links)

    # Close the CSV writers
    documents_writer.close()
    entities_writer.close()
    links_writer.close()

    # Build the JSON config and save as a JSON file
    config = build_config(num_entities_files,
                          num_documents_files, num_links_files)
    write_config_json(output_folder, config)

    # Make the skip entities file
    skip_entities_filepath = os.path.join(data_folder, "skip_entities.txt")
    with open(skip_entities_filepath, 'w') as fp:
        pass
