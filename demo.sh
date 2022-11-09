echo "Running Shortest Path App in demo mode"

OUTPUT_FOLDER=./working/

# Clean the working folder
rm -f ./${OUTPUT_FOLDER}/*

# Run the web-app
./web-app -data=./test-data-sets/set-1/data-config.json -i2=./test-data-sets/set-1/i2-config.json -folder=${OUTPUT_FOLDER}
