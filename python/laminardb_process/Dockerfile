FROM python:3.13.1-slim

WORKDIR /opt/laminardb-process
COPY python/laminardb_process/requirements.lock ./requirements.lock
RUN python -m pip install --no-cache-dir -r requirements.lock

COPY python/laminardb_process/pyproject.toml ./pyproject.toml
COPY python/laminardb_process/laminardb_process ./laminardb_process
RUN python -m pip install --no-cache-dir --no-build-isolation --no-deps .

COPY examples/process_python/handler.py ./handler.py
COPY examples/process_python/manifest.json ./manifest.json
ENV PYTHONUNBUFFERED=1 OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1
ENTRYPOINT ["laminardb-process-worker"]
CMD ["--manifest", "manifest.json", "--handler", "handler:handle", "--bind", "127.0.0.1:50051"]
