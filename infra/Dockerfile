# One image with every entry point; docker-compose picks the binary per service.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN for b in dispatcher sender webhook janitor migrate; do \
      CGO_ENABLED=0 go build -trimpath -o /out/$b ./cmd/$b || exit 1; \
    done

FROM alpine:3.20
COPY --from=build /out/ /usr/local/bin/
COPY infra/providers.yaml /etc/eshaar/providers.yaml
USER nobody
