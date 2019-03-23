FROM alpine:3.8

# Grab proxy settings to allow internet access.
ENV http_proxy=$HTTP_PROXY
ENV https_proxy=$HTTPS_PROXY

# Install required packages.
RUN apk --update add py3-psycopg2

RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install --upgrade pip
RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install flask

# Configure Flask server.
ENV FLASK_APP=/src/server.py
ENV FLASK_ENV=development
ENV FLASK_DEBUG=1

WORKDIR /src
COPY ./src /src

# Check source for syntax errors. Will not catch runtime errors.
RUN find /src -name "*.py" | xargs python3 -m py_compile

# Configure command to run on startup.
CMD ["flask", "run", "--host=0.0.0.0"]
