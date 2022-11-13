# Shortest path web-app

## Quick demo

Build and test the executable using

```bash
./build.sh
```

and then run the demo using

```bash
./demo.sh
```

The demo uses one of the small test datasets, but it's enough to show the functionality. Navigate to http://localhost:8090 to access the web-app.

## Build the Docker image

The `Dockerfile` in this project builds a minimal image in two stages. To build and run:

```bash
docker compose build
docker compose up
```

## Location of the data files

The CSV and TXT files containing data from which to build the graph must be placed
in a `data` sub-directory where the `data-config.json` file is placed. This convention-over-
configuration approach is taken to make the `data-config.json` file simpler.

The configuration for the i2 generation should be stored in `i2-config.json`.
