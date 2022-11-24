# Encapsulate a field in double quotes.
function encapsulate(field) {
	return "\"" field "\""
}

# Escape double quotes in a field.
function escape_double_quote(field) {
	gsub("\"", "\\\"", field)
	return field
}

BEGIN {
	FS = "|";   # Set the field separator for the input
	OFS = ",";  # Set the output field separator
};
{
	# Skip blank lines
	if (NF == 0) {
		next
	}

	# Check the line has three fields and if it does not,
	# then print a message to stderr
	if (NF != 3) {
		printf "Error! %d field(s) on line: %s\n", NF, $0 > "/dev/stderr"
		next
	}

	# Prepare the fields
	for (idx=1; idx<=3; idx++) {
		output[idx] = encapsulate(escape_double_quote($idx))
	}

	# Return the converted fields
	print output[1], output[2], output[3]
}

