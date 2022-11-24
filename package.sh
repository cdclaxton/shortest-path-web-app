#!/bin/bash

# This script builds and packages everything required for transferring to the system on which
# it will deployed.

# --------------------------------------------------------------------------------------------------
# Parameters
# --------------------------------------------------------------------------------------------------

# Location for the folder for populating the output zip file
folder=/c/Users/cdcla/Downloads/shortest-path

output_file=shortest-path.zip

# --------------------------------------------------------------------------------------------------
# Script
# --------------------------------------------------------------------------------------------------

# If the folder doesn't exist, then make it
if [ ! -d $folder ]; then
    echo Making output folder $folder
    mkdir $folder
else
    echo Output folder $folder found
fi

# Build the Docker image
echo "Building the Shortest Path web-app Docker image ..."
docker-compose build

# If the build failed, then stop
if [ $? -ne 0 ]; then
    echo "Build failed. Packinging stopped"
    exit 1
fi

# Save the Docker image to the output folder
echo "Saving the Docker image ..."
docker save shortestpath:latest -o $folder/shortest-path.docker

if [ $? -ne 0 ]; then
    echo "Saving Docker image failed. Packaging stopped"
    exit 1
fi

# Copy the readme
echo "Copying readme ..."
cp readme.md $folder

# Create a scripts folder
scripts_folder=$folder/scripts

if [ ! -d $scripts_folder ]; then
    echo "Making scripts folder $scripts_folder"
    mkdir $scripts_folder
else
    echo "Scripts folder $scripts_folder found"
fi

# Copy the scripts to the folder
scripts=( ./scripts/data_config_generator/nr6.py ./scripts/convert.awk ./scripts/convert_files.sh )

for file in "${scripts[@]}"; do
    echo "Copying $file"
    cp $file $scripts_folder
    if [ $? -ne 0 ]; then
        echo "Failed to copy file $file"
        exit 1
    fi
done;

# Zip up the folder
echo "Zipping package ..."
cd $folder/..
zip -r $output_file shortest-path

if [ $? -ne 0 ]; then
    echo "Zipping failed. Packinging stopped"
    exit 1
fi

cd -

echo "Packaging complete"
