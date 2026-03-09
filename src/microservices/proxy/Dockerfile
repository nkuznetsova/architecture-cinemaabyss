FROM python:3.12-slim

# Install app python requirements.
COPY requirements.txt /tmp/

RUN \
    pip install --no-deps -r /tmp/requirements.txt \
    && rm -rf \
        /tmp/*requirements.txt \
        /var/tmp/* \
        /root/.cache

WORKDIR /proxy

COPY . /proxy

CMD ["python", "app/main.py"]
