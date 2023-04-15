# syntax=docker/dockerfile:1

# --------------------------------------------------------------------
# STEP 1: Build
# --------------------------------------------------------------------

FROM golang:1.19-buster AS build

# Create a working directory inside the image
WORKDIR /app

# Download dependencies
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Copy the entire project to the working directory
COPY . ./

# Run the tests
RUN go test ./...

# Build the executable
RUN CGO_ENABLED=0 GOOS=linux go build -v -o ./web-app ./cmd/app

# --------------------------------------------------------------------
# STEP 2: Deploy
# --------------------------------------------------------------------

FROM scratch

WORKDIR /

# Copy the built executable from the first stage image
COPY --from=build /app/web-app .

# Port on which the application runs
EXPOSE 8090

# Set the executable to run when the container runs
ENTRYPOINT [ "./web-app", "-data=/data/data-config.json", "-i2=/data/i2-config.json", "-i2spider=/data/i2-spider-config.json", "-folder=/data/chartFolder", "-message=/data/message.html"]
