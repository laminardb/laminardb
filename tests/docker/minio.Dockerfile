# Build the same releases used by the Iceberg fixture. Their upstream container
# images are no longer publicly available, so CI builds from immutable commits.
FROM golang:1.23.2-alpine3.20 AS build
RUN apk add --no-cache git
ENV CGO_ENABLED=0
RUN git clone https://github.com/minio/minio.git /src/minio \
    && cd /src/minio \
    && git checkout d10bb7e1b667c2df72c394ef1fa52ab4a6802d0f \
    && go build -trimpath -ldflags='-s -w' -o /out/minio .
RUN git clone https://github.com/minio/mc.git /src/mc \
    && cd /src/mc \
    && git checkout cf128de2cf42e763e7bd30c6df8b749fa94e0c10 \
    && go build -trimpath -ldflags='-s -w' -o /out/mc .

FROM alpine:3.20.3
RUN apk add --no-cache ca-certificates
COPY --from=build /out/minio /out/mc /usr/bin/
ENTRYPOINT ["minio"]
