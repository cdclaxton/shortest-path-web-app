echo "Running Shortest Path App in demo mode (in-memory)"

OUTPUT_FOLDER=./working/i2/

# Dataset within ./test-data-sets/ to use
SET=set-2

# Make the output folder if it doesn't exist or clean it if it does
if [[ ! -d ${OUTPUT_FOLDER} ]]; then
    echo "i2 chart folder doesn't exist. Making it at: ${OUTPUT_FOLDER}"
    mkdir -p ${OUTPUT_FOLDER}
else 
    echo "i2 chart folder found. Cleaning it"
    rm -f ./${OUTPUT_FOLDER}/*
fi

# Run the web-app
./web-app -data=./test-data-sets/$SET/data-config.json -i2=./test-data-sets/$SET/i2-config.json -i2spider=./test-data-sets/$SET/i2-spider-config.json -folder=${OUTPUT_FOLDER} -message=./test-data-sets/$SET/message.html
