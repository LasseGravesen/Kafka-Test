FROM python:3.7.2-stretch

RUN apt-get update \
    && apt-get install -y build-essential \
    && curl -fSL https://github.com/edenhill/librdkafka/archive/v0.11.6.tar.gz -o /tmp/librdkafka.tar.gz \
    && tar xvzf /tmp/librdkafka.tar.gz -C /tmp \
    && cd /tmp/librdkafka-0.11.6 \
    && ./configure \
    && make \
    && make install \
    && rm -r /tmp/librdkafka-0.11.6 \
    && rm /tmp/librdkafka.tar.gz \
    && ldconfig \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir /app

COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip --no-cache
RUN pip install -r requirements.txt --no-cache

COPY main.py ./app/main.py

CMD ["python3", "./app/main.py"]
