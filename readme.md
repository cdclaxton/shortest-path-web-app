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

The demo uses one of the small test datasets, but it's enough to show the functionality.
Navigate to http://localhost:8090 to access the web-app.

## Build the Docker image

The `Dockerfile` in this project builds a minimal image in two stages. To build the image and run
with simple test data:

```bash
docker compose build
docker compose up
```

## Location of the data files

The CSV and TXT files containing data from which to build the graph must be placed
in a `data` sub-directory where the `data-config.json` file is placed. This convention-over-
configuration approach is taken to make the `data-config.json` file simpler.

The configuration for the i2 generation should be stored in `i2-config.json`.

## Enhancements

### Pebble

During initial testing with a large volume of data it was found that the ingest time was very high,
prohibitively so. The Pebble backend was found to be the cause, and so a benchmark test was written
in the `graphstore` package. The benchmark can be run with:

`go test -run=Bench -bench=. -benchtime=10x`

Tests where 10,000 entities were added to the bipartite graph Pebble backend were performed with and
without synchronisation. The results were:

| Synchronisation | Speed (ns/op)  |
| --------------- | -------------- |
| Sync            | 24,973,822,020 |
| NoSync          | 258,309,370    |

Therefore, turning synchronisation off yielded a 97 times speed up.
