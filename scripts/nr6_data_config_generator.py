# Generate the data configuration as a JSON file where the network data is modelled as:
#
#    |-----|\
#    |     |-\                            0
#    |     |--\                          /|\
#    |        | ------[ link ]------    / | \
#    |        |                          /\
#    |--------|                         /  \
#
#     Document                         Entity
#
# The document CSV files have the form:
#   * Filename: <prefix>_doc_<doc-type>.csv
#   * Fields: document_id|document_date|document_label
#
# The link CSV files have the form:
#   * Filename: <prefix>_entities_<entity-type>.csv
#   * Fields: entity_id,document_id
#
# The entity CSV files have the form:
#   * Filename: <prefix>_entities_<entity-type>_labels.csv
#   * Fields: entity_id,label

import json


def build_entity_files(prefix, entity_types):
    """Build the config for CSV files containing entities."""

    # Preconditions
    assert type(prefix) == str, f"Expected a str, got {type(prefix)}"
    assert type(
        entity_types) == dict, f"Expected a dict, got {type(entity_types)}"

    return [{
        "path": f"{prefix}_entities_{short.lower()}_labels.csv",
        "entityType": long,
        "delimiter": ",",
        "entityIdField": "entity_id",
        "fieldToAttribute": {
            "label": "label"
        }
    } for short, long in entity_types.items()]


def build_document_files(prefix, doc_types):

    # Preconditions
    assert type(prefix) == str, f"Expected a str, got {type(prefix)}"
    assert type(
        doc_types) == dict, f"Expected a dict, got {type(doc_types)}"

    return [{
        "path": f"{prefix}_doc_{short.lower()}.csv",
        "documentType": long,
        "delimiter": "|",
        "documentIdField": "document_id",
        "fieldToAttribute": {
            "document_label": "label",
            "document_date": "date"
        }
    } for short, long in doc_types.items()]


def build_link_files(prefix, entity_types):

    # Preconditions
    assert type(prefix) == str, f"Expected a str, got {type(prefix)}"
    assert type(
        entity_types) == list, f"Expected a list, got {type(entity_types)}"

    return [{
        "path": f"{prefix}_entities_{tpe.lower()}.csv",
        "entityIdField": "entity_id",
        "documentIdField": "document_id",
        "delimiter": ","
    } for tpe in entity_types]


def build_config(prefix, doc_types, entity_types, skip_entities_filename, dockerised):
    """Build data config for the shortest path web-app."""

    # Preconditions
    assert type(prefix) == str, f"Expected a str, got {type(prefix)}"
    assert type(
        doc_types) == dict, f"Expected a dict, got {type(doc_types)}"
    assert type(
        entity_types) == dict, f"Expected a dict, got {type(entity_types)}"
    assert type(
        skip_entities_filename) == str, f"Expected a str, got {type(prefix)}"
    assert type(dockerised) == bool, f"Expected a bool, got {type(dockerised)}"

    # Build the config
    entity_config = build_entity_files(prefix, entity_types)
    assert type(entity_config) == list

    document_config = build_document_files(prefix, doc_types)
    assert type(document_config) == list

    links_config = build_link_files(prefix, list(entity_types.keys()))
    assert type(links_config) == list

    if dockerised:
        bipartiteFolder = "/pebble/bipartite"
        unipartiteFolder = "/pebble/unipartite"
    else:
        bipartiteFolder = "./working/bipartitePebble/"
        unipartiteFolder = "./working/unipartitePebble/"

    return {
        "graphData": {
            "entitiesFiles": entity_config,
            "documentsFiles": document_config,
            "linksFiles": links_config,
            "skipEntitiesFile": skip_entities_filename
        },
        "bipartiteGraphConfig": {
            "type": "pebble",
            "folder": bipartiteFolder,
            "deleteFilesInFolder": True
        },
        "unipartiteGraphConfig": {
            "type": "pebble",
            "folder": unipartiteFolder,
            "deleteFilesInFolder": True
        }
    }


if __name__ == '__main__':

    prefix = "nr6"
    doc_types = {"A_01": "Doc A", "B_02": "Doc B"}
    entity_types = {"ADR": "Address", "IND": "Individual"}
    skip_entities_filename = "skip_entities.txt"
    dockerised = False

    output_filepath = "./test-data-sets/set-4/data-config.json"

    config = build_config(prefix, doc_types, entity_types,
                          skip_entities_filename, dockerised)

    print(f"Writing config to {output_filepath}")
    with open(output_filepath, 'w') as fp:
        fp.write(json.dumps(config, indent=4))
