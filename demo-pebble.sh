echo "Running Shortest Path App in demo mode (Pebble backend)"

# Paths referenced in the data-config.json file
OUTPUT_FOLDER=./working/i2/
BIPARTITE_FOLDER=./working/bipartitePebble
UNIPARTITE_FOLDER=./working/unipartitePebble

# Dataset within ./test-data-sets/ to use
SET=set-3

# Make a folder if it doesn't exist or remove its contents if it does.
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

# Make the folders if they don't exist or clean them if they do
makeOrCleanFolder ${OUTPUT_FOLDER} "i2 chart"
makeOrCleanFolder ${BIPARTITE_FOLDER} "Bipartite graph"
makeOrCleanFolder ${UNIPARTITE_FOLDER} "Unipartite graph"

# Run the web-app
./web-app -data=./test-data-sets/$SET/data-config.json -i2=./test-data-sets/$SET/i2-config.json -folder=${OUTPUT_FOLDER} -message=./test-data-sets/$SET/message.html
