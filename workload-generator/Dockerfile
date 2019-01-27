FROM alpine:3.8

# Install required packages.
RUN apk --update add python3
RUN apk add --no-cache bash
RUN apk add nmap
RUN pip3 install --upgrade pip
RUN pip3 install requests
RUN pip3 install tornado 
RUN pip3 install aiohttp