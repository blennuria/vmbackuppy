FROM victoriametrics/vmbackup:latest AS vmbackup
FROM victoriametrics/vmrestore:latest AS vmrestore

FROM python:3.14-slim

COPY --from=vmbackup /vmbackup-prod /usr/local/bin/vmbackup
COPY --from=vmrestore /vmrestore-prod /usr/local/bin/vmrestore

WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir .

COPY . .

ENTRYPOINT ["python", "main.py"]
