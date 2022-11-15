
# Run the tests
echo Running tests ...
go test ./...
testResult=$?

if [[ $testResult -ne 0 ]]; then
    echo Tests failed. Stopping build.
    exit -1
fi

# Build the executable
echo Building executable ...
CGO_ENABLED=0 go build -v -o ./web-app ./cmd/app
buildResult=$?

if [[ $buildResult -ne 0 ]]; then
    echo Build failed
    exit -1
else
    echo Build succeeded
fi
