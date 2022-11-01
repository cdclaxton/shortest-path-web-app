
# Run the tests
echo Running tests ...
go test ./...
testResult=$?

if [[ $testResult -ne 0 ]]; then
    echo Tests failed. Stopping build.
    exit
fi

# Build the executable
echo Building executable ...
go build -v -o ./web-app ./cmd/app
buildResult=$?

if [[ $buildResult -ne 0 ]]; then
    echo Build failed
else
    echo Build succeeded
fi
