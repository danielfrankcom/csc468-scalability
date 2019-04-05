FROM python:3.7-alpine

# Grab proxy settings to allow internet access.
ENV http_proxy=$HTTP_PROXY
ENV https_proxy=$HTTPS_PROXY
RUN apk update && apk add postgresql-dev gcc python3-dev musl-dev
RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install --upgrade pip 
RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install pika==0.13.1
RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install psycopg2


WORKDIR /src

CMD ["python", "/src/consumer.py"]

COPY ./src /src

# Check source for syntax errors. Will not catch runtime errors.
RUN find /src -name "*.py" | xargs python3 -m py_compile