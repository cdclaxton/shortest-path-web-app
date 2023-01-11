echo "Running Shortest Path App in demo mode with dataset 4"

OUTPUT_FOLDER=./working/i2/

# Make the output folder if it doesn't exist or clean it if it does
if [[ ! -d ${OUTPUT_FOLDER} ]]; then
    echo "i2 chart folder doesn't exist. Making it at: ${OUTPUT_FOLDER}"
    mkdir -p ${OUTPUT_FOLDER}
else 
    echo "i2 chart folder found. Cleaning it"
    rm -f ./${OUTPUT_FOLDER}/*
fi

# Run the web-app
./web-app -data=./test-data-sets/set-4/data-config.json -i2=./test-data-sets/set-4/i2-config.json -folder=${OUTPUT_FOLDER} -message=./test-data-sets/set-4/message.html
