# syntax=docker/dockerfile:1
# just wraps the static binary from CI, ctx = dist/ (dist/<os>/<arch>/dryrun)
FROM gcr.io/distroless/static-debian13:nonroot@sha256:8ce6dd87e9823c5b48dcfb2c8604714ccb351ce59607327bcc4957448da279b5

ARG TARGETOS TARGETARCH

# artifacts lose +x
COPY --chmod=0755 ${TARGETOS}/${TARGETARCH}/dryrun /usr/local/bin/dryrun

# /home/nonroot is 0700, breaks w/ --user
ENV DOCKER_CONFIG=/docker
WORKDIR /work
ENTRYPOINT ["/usr/local/bin/dryrun"]
