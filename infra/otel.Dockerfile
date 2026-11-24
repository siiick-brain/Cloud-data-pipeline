# Collector image plus a static busybox so compose can run a healthcheck in a distroless image.
FROM busybox:1.37.0-musl AS bb
FROM otel/opentelemetry-collector-contrib:0.165.0
COPY --from=bb /bin/busybox /busybox
