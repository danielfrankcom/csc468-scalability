FROM alpine:3.8

# Grab proxy settings to allow internet access.
ENV http_proxy=$HTTP_PROXY
ENV https_proxy=$HTTPS_PROXY

# Install dependencies.
RUN apk --update add openjdk8

WORKDIR /src
COPY ./src /src

#-Dhttp.proxyHost=192.168.1.1 -Dhttp.proxyPort=3128
RUN ./gradlew build
