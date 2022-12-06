# Apache HTTPD proxy

## Extract the configuration files

To extract the default config files from the image:

```bash
docker run --rm httpd:2.4.54-alpine cat /usr/local/apache2/conf/httpd.conf > proxy/httpd.conf
docker run --rm httpd:2.4.54-alpine cat /usr/local/apache2/conf/extra/proxy-html.conf > proxy/proxy-html.conf
```

## Build and run

```bash
# Just build the Apache HTTPD Docker image
docker build -t httpd-custom -f Dockerfile-httpd .

# Run the Apache HTTPD reverse proxy and the web-app
docker-compose -f docker-compose-httpd.yml up
```
