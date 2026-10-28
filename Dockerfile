# syntax=docker/dockerfile:1
# just wraps the static binary from CI, ctx = dist/ (dist/<os>/<arch>/dryrun)
FROM gcr.io/distroless/static-debian13:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99

ARG TARGETOS TARGETARCH

# artifacts lose +x
COPY --chmod=0755 ${TARGETOS}/${TARGETARCH}/dryrun /usr/local/bin/dryrun

# /home/nonroot is 0700, breaks w/ --user
ENV DOCKER_CONFIG=/docker
WORKDIR /work
ENTRYPOINT ["/usr/local/bin/dryrun"]
