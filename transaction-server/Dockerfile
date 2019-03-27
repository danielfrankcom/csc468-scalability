FROM python:3.7-alpine

# Grab proxy settings to allow internet access.
ENV http_proxy=$HTTP_PROXY
ENV https_proxy=$HTTPS_PROXY

# Install required packages.
RUN apk --update add gcc
RUN apk --update add libc-dev
RUN apk --update add make

RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install --upgrade pip
RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install pika
RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install aiohttp
RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install asyncpg
RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install quart
RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install hypercorn[uvloop]

WORKDIR /src

# Configure command to run on startup.
#CMD ["hypercorn", "server:app", "--bind", "0.0.0.0:5000", "--worker-class", "uvloop"]
CMD ["python", "/src/server.py"]

COPY ./src /src

# Check source for syntax errors. Will not catch runtime errors.
RUN find /src -name "*.py" | xargs python3 -m py_compile
