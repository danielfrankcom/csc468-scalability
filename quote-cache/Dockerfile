FROM alpine:3.8

# Use these variables in the lab environment
#ENV http_proxy=http://192.168.1.1:3128/
#ENV https_proxy=http://192.168.1.1:3128/

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
