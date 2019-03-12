FROM alpine:3.8

# Grab proxy settings to allow internet access.
ENV http_proxy=$HTTP_PROXY
ENV https_proxy=$HTTPS_PROXY

# Install required packages.
RUN apk --update add py3-psycopg2

RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install --upgrade pip
RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install flask
RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install gunicorn

# Configure Flask server.
ENV FLASK_APP=/src/server.py
ENV FLASK_ENV=development
ENV FLASK_DEBUG=1

WORKDIR /src
COPY ./src /src

# Configure command to run on startup.
#CMD ["gunicorn", "--bind", "0.0.0.0:5000","--workers=1","--preload","wsgi:application"]
CMD ["flask", "run", "--host", "0.0.0.0"]
