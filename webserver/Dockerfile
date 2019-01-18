FROM alpine:3.8

# Install required packages.
RUN apk --update add python3
RUN pip3 install --upgrade pip
RUN pip3 install flask

# Configure Flask server.
ENV FLASK_APP=/src/server.py
ENV FLASK_ENV=development
ENV FLASK_DEBUG=1

# Configure command to run on startup.
CMD ["flask", "run", "-h", "0.0.0.0"]
