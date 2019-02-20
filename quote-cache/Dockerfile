FROM alpine:3.8

# Grab proxy settings to allow internet access.
ENV http_proxy=$HTTP_PROXY
ENV https_proxy=$HTTPS_PROXY

# Install dependencies.
RUN apk --update add build-base g++ cmake
RUN apk --update add boost-dev

# Performed here to prevent unnecessary intermediate containers.
WORKDIR /build
CMD ["/out/quote-cache"]

# Build the source.
COPY ./src /src
RUN cmake /src
RUN cmake --build .
