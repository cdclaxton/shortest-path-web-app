# i2 Chart Generation

The i2 chart generation is performed by creating a CSV file that is then
imported into i2 Analyst Notebook using a pre-defined input specification.

The generator creates lines in a CSV file that have the structure:

`<Entity 1> --- <Link> --- <Entity 2>`

Note that the documents are not present directly as they tend to clutter
the resulting chart. Instead, they are summarised in the label of a link.

## JSON configuration

The top-level structure of the JSON configuration to generate the i2
charts is:

```json
{
  "columns": [],
  "entities": [],
  "links": {}
}
```

The `columns` list contains the ordered list of entity columns in the CSV file. The `entities` list contains the field configuration for each entity type.

Each entity type within the JSON configuration has the structure:

```json
{
  "type": "<Entity type>",
  "columns": {}
}
```

where `<Entity type>` is the type of the entity (as imported). The `columns` map contains the specification of the contents of each field
in the CSV file.

The in-built constants are:

- `<ID>` -- entity ID
- `<ENTITY-SET-NAMES>` -- comma-separated list of entity set names
- `<DOCUMENT-TYPES>` -- comma-separated list of document types
- `<DOCUMENT-DATE-RANGE>` -- document date range
