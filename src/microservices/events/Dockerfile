FROM python:3.12-slim

# Install app python requirements.
COPY requirements.txt /tmp/

RUN \
    pip install --no-deps -r /tmp/requirements.txt \
    && rm -rf \
        /tmp/*requirements.txt \
        /var/tmp/* \
        /root/.cache

WORKDIR /events

COPY . /events

#CMD ["uvicorn", "app.main:app", "--reload"]
CMD ["python", "-m", "app.main"]
