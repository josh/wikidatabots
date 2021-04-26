FROM python:3.9.4-alpine AS builder

RUN apk add --no-cache build-base

WORKDIR /usr/src/app

RUN python -m pip install --upgrade pip
COPY requirements.txt ./
RUN pip install --user -r requirements.txt

COPY *.sh *.py ./
RUN python -m compileall .


FROM python:3.9.4-alpine

WORKDIR /usr/src/app

RUN wget -O /usr/bin/tickerd https://github.com/josh/tickerd/releases/latest/download/tickerd-linux-amd64 && chmod +x /usr/bin/tickerd

COPY --from=builder /root/.local /root/.local
COPY --from=builder /usr/src/app /usr/src/app

ENTRYPOINT [ "/usr/bin/tickerd", "--", "/usr/src/app/entrypoint.sh" ]

ENV TICKERD_HEALTHCHECK_FILE "/var/run/healthcheck"
HEALTHCHECK --interval=1m --timeout=3s --start-period=3s --retries=1 \
  CMD [ "/usr/bin/tickerd", "-healthcheck" ]
