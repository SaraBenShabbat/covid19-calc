FROM python:3.8

ENV TMPDIR=/var/tmp

WORKDIR /usr/local/bin

COPY requirements.txt .

COPY scoring.py .

RUN pip install --upgrade pip && \
    pip install --no-cache-dir certifi && \
    pip install --no-cache-dir -r requirements.txt

CMD [ "python3", "./scoring.py" ]

