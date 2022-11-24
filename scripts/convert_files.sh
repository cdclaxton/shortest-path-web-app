
# Array of files to convert from pipe separated to comma separated
files=( test-data.csv test-data-2.csv )

# Walk through each file
for file in "${files[@]}"; do
	echo "Processing file $file"
	
	# Convert the file
	awk -f convert.awk $file > temp.csv

	# Move the converted file
	mv temp.csv $file
done;

