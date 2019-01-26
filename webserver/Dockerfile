FROM alpine:3.8

# Install required packages.
RUN apk --update add python3 py-gevent
RUN pip3 install --upgrade pip
RUN pip3 install flask
RUN pip3 install gunicorn
RUN pip3 install gevent

# Configure Flask server.
ENV FLASK_APP=/src/server.py
ENV FLASK_ENV=development
ENV FLASK_DEBUG=1

EXPOSE 3000


WORKDIR /src
# Configure command to run on startup.
CMD ["gunicorn", "--bind", "0.0.0.0:3000","--worker-connections=20000","--worker-class=gevent","wsgi"]
