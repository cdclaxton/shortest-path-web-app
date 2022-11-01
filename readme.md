# Shortest path web-app

## Building

To build the Shortest Path application:

```
go build -v -o ./app ./cmd/app
```

## Running the application

```bash
./app <folder>
```

## Location of the data files

The CSV and TXT files containing data from which to build the graph must be placed
in a `data` sub-directory where the `data-config.json` file is placed. This convention-over-
configuration approach is taken to make the `data-config.json` file simpler.

The configuration for the i2 generation should be stored in `i2-config.json`.
