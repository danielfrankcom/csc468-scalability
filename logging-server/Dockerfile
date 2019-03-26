FROM python:3.7-alpine

# Grab proxy settings to allow internet access.
ENV http_proxy=$HTTP_PROXY
ENV https_proxy=$HTTPS_PROXY

RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install --upgrade pip 
RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install pika==1.0.0b2
RUN pip3 --trusted-host pypi.org --trusted-host files.pythonhosted.org install psycopg2

EXPOSE 5672

WORKDIR /src

CMD ["python", "/src/consumer.py"]

COPY ./src /src

# Check source for syntax errors. Will not catch runtime errors.
RUN find /src -name "*.py" | xargs python3 -m py_compile