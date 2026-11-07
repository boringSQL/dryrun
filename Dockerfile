# syntax=docker/dockerfile:1
# just wraps the static binary from CI, ctx = dist/ (dist/<os>/<arch>/dryrun)
FROM gcr.io/distroless/static-debian13:nonroot@sha256:25ac8f0c3b2334a9eb05452266bd4697864a8c8ef0ef71e5e6480d7f92868329

ARG TARGETOS TARGETARCH

# artifacts lose +x
COPY --chmod=0755 ${TARGETOS}/${TARGETARCH}/dryrun /usr/local/bin/dryrun

# /home/nonroot is 0700, breaks w/ --user
ENV DOCKER_CONFIG=/docker
WORKDIR /work
ENTRYPOINT ["/usr/local/bin/dryrun"]
