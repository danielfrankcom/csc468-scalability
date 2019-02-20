FROM alpine:3.8

# Grab proxy settings to allow internet access.
ENV http_proxy=$HTTP_PROXY
ENV https_proxy=$HTTPS_PROXY

RUN apk --update add openjdk8

WORKDIR /src
CMD ["/src/entry"]
