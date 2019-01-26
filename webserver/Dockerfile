FROM alpine:3.8

# Install required packages.
RUN apk --update add python3
RUN pip3 install --upgrade pip
RUN pip3 install flask
RUN pip3 install gunicorn

# Configure Flask server.
ENV FLASK_APP=/src/server.py
ENV FLASK_ENV=development
ENV FLASK_DEBUG=1


WORKDIR /src
# Configure command to run on startup.
CMD ["gunicorn", "--bind", "0.0.0.0:5000","--workers=30","wsgi:application"]
