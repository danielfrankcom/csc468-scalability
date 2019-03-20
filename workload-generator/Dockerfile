FROM alpine:3.8

# Grab proxy settings to allow internet access.
ENV http_proxy=$HTTP_PROXY
ENV https_proxy=$HTTPS_PROXY

# Install dependencies.
RUN apk --update add openjdk8 nss

WORKDIR /src

# Each 'RUN' instance spawns its own shell, so store output
# of commands into files for access later.
RUN echo $http_proxy | grep -oE [0-9.]+ | sed -n 1p > /host
RUN echo $http_proxy | grep -oE [0-9.]+ | sed -n 2p > /port

COPY ./src/gradlew /src/gradlew
COPY ./src/gradle /src/gradle

# Cache gradle zip file
RUN ./gradlew -Dhttp.proxyHost=$(cat /host) -Dhttp.proxyPort=$(cat /port)

# Copy source code here to take advantage of Docker caching.
COPY ./src /src
RUN ./gradlew -Dhttp.proxyHost=$(cat /host) -Dhttp.proxyPort=$(cat /port) build
