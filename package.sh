#!/bin/bash

# This script builds and packages everything required for transferring to the system on which
# the web-app will deployed.

# --------------------------------------------------------------------------------------------------
# Parameters
# --------------------------------------------------------------------------------------------------

# Location for the folder for populating the output zip file
folder=/c/Users/cdcla/Downloads/shortest-path

# Name of the Zip file to create
output_file=shortest-path.zip

# --------------------------------------------------------------------------------------------------
# Script
# --------------------------------------------------------------------------------------------------

# If the folder doesn't exist, then make it
if [ ! -d $folder ]; then
    echo "Making output folder ${folder}"
    mkdir $folder
else
    echo "Output folder ${folder} found"
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

# Copy the scripts
echo "Copying scripts ..."
cp -r ./scripts $folder

# Copy the images to the folder
echo "Copying images ..."
cp -r ./images $folder

# Copy the proxy config to the folder
echo "Copying Apache HTTPD proxy config ..."
cp -r ./proxy $folder

# Copy the test datasets to the folder
echo "Copying test datasets ..."
cp -r ./test-data-sets $folder

# Zip up the folder
echo "Zipping package ..."
cd $folder/..
zip -r $output_file shortest-path

if [ $? -ne 0 ]; then
    echo "Zipping failed. Packinging stopped"
    exit 1
fi

echo "Packaging complete"
