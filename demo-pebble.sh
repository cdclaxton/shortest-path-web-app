echo "Running Shortest Path App in demo mode (Pebble backend)"

OUTPUT_FOLDER=./working/i2/
BIPARTITE_FOLDER=./working/bipartitePebble
UNIPARTITE_FOLDER=./working/unipartitePebble

makeOrCleanFolder () {

    # Preconditions
    if [ $# -ne 2 ]; then
        echo "Error: Incorrect number of arguments"
        exit 1
    fi

    # Extract the arguments
    folder=$1
    name=$2

    if [ ! -d ${folder} ]; then
        echo "${name} folder doesn't exist. Making it at: ${folder}"
        mkdir -p ${folder}
    else    
        echo "${name} folder found. Cleaning it"
        rm -f ./${folder}/*
    fi
}

# Make the output folder if it doesn't exist or clean it if it does
makeOrCleanFolder ${OUTPUT_FOLDER} "i2 chart"
makeOrCleanFolder ${BIPARTITE_FOLDER} "Bipartite graph"
makeOrCleanFolder ${UNIPARTITE_FOLDER} "Unipartite graph"

# Run the web-app
./web-app -data=./test-data-sets/set-1/data-config-pebble.json -i2=./test-data-sets/set-1/i2-config.json -folder=${OUTPUT_FOLDER}
