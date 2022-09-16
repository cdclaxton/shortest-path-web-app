# i2 Chart Generation

The i2 chart generation is performed by creating a CSV file that can be imported into i2 Analyst
Notebook using a pre-defined input specification.

The generator creates lines in a CSV file that have the structure:

`<Entity 1> --- <Link> --- <Entity 2>`

Note that the documents are not present directly as they tend to clutter the resulting chart.
Instead, they are summarised in the label of a link between two entities.

## JSON configuration

The top-level structure of the JSON configuration to generate the i2 charts is:

```json
{
  "columns": [],
  "entities": {},
  "links": {},
  "attributeNotKnown": ""
}
```

The `columns` list is the ordered list of columns for each entity in the CSV file.

The `entities` map contains the field configuration for each entity type. The fields for a given
entity type must match those in the `columns` list. The field configuration is a mixture of
free-text and placeholders that are populated from the data.

The in-built constants are:

- `<ID>` -- entity ID
- `<ENTITY-SET-NAMES>` -- comma-separated list of entity set names (typically used to indicate
  why the entity is of interest)
- `<DOCUMENT-TYPES>` -- comma-separated list of document types connecting two entities
- `<DOCUMENT-DATE-RANGE>` -- document date range

Each entity attribute is also available. For example, if a person entity has the attribute
`Surname` then the keyword `<Surname>` can be used and it will be populated with the value from
the data.

The `links` map is expected to contain the key-value pairs:

```json
{
  "label": "",
  "dateAttribute": "",
  "dateFormat": ""
}
```

`label` specifies the construction of a link between two entities, `dateAttribute` is the name
of the attribute for a document that contains the date and `dateFormat` specifies the date format
in Golang's time format.

The `attributeNotKnown` field is a string that is used when a keyword is not known. This can happen
when there is a typo in the keyword or the entity doesn't contain the expected attribute.

## Example JSON configuration

Suppose the data is composed of two types of entities, namely Person and Address. The attributes
for a Person are `Surname` and `Forename`. An Address has attributes `First line`, `City`, `Country`. The following JSON configuration could be used:

```json
{
  "columns": ["icon", "id", "label", "entitySets", "description"],
  "entities": {
    "Person": {
      "icon": "Person",
      "id": "Person-<ID>",
      "label": "<Surname>, <Forename> [<ENTITY-SET-NAMES>]",
      "entitySets": "<ENTITY-SET-NAMES>",
      "description": "Person <Forename> <Surname> can be found at http://network-display/<ID>"
    },
    "Address": {
      "icon": "Location",
      "id": "Address-<ID>",
      "label": "<First line>, <City>, <Country> [<ENTITY-SET-NAMES>]",
      "entitySets": "<ENTITY-SET-NAMES>",
      "description": "Address can be found at http://network-display/<ID>"
    }
  },
  "links": {
    "label": "<NUM-DOCS> docs (<DOCUMENT-TYPES> <DOCUMENT-DATE-RANGE>)",
    "dateAttribute": "Date",
    "dateFormat": "02/01/2006"
  },
  "attributeNotKnown": "Unknown"
}
```
