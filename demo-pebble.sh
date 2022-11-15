echo "Running Shortest Path App in demo mode (Pebble backend)"

OUTPUT_FOLDER=./working/i2/

# Clean the working folder
rm -f ./${OUTPUT_FOLDER}/*

# Run the web-app
./web-app -data=./test-data-sets/set-1/data-config-pebble.json -i2=./test-data-sets/set-1/i2-config.json -folder=${OUTPUT_FOLDER}
