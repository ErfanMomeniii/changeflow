# syntax=docker/dockerfile:1
FROM golang:1.26 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown
RUN CGO_ENABLED=0 go build -trimpath \
	-ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" \
	-o /out/changeflow ./cmd/changeflow

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/changeflow /usr/local/bin/changeflow

USER nonroot:nonroot
EXPOSE 9187

ENTRYPOINT ["/usr/local/bin/changeflow"]
CMD ["run", "-c", "/etc/changeflow/changeflow.yaml"]
