FROM python:3.12-alpine
RUN addgroup -S -g 10001 graphdb && adduser -S -D -u 10001 -G graphdb graphdb \
    && mkdir -m 0700 /state && mkdir -m 0755 /metrics && chown graphdb:graphdb /state /metrics
WORKDIR /opt/graphdb
COPY scripts/backup_automation.py scripts/backup_automation.py
COPY sdk/python/graphdb_sdk sdk/python/graphdb_sdk
USER graphdb
ENTRYPOINT ["python3", "scripts/backup_automation.py"]
