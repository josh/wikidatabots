FROM python:3.10.2-alpine

RUN apk add --no-cache build-base libxml2 libxslt-dev

RUN wget -O /usr/bin/tickerd https://github.com/josh/tickerd/releases/latest/download/tickerd-linux-amd64 && chmod +x /usr/bin/tickerd

WORKDIR /usr/src/app

RUN python -m pip install --upgrade pip
COPY requirements.txt ./
RUN pip install --user -r requirements.txt

COPY *.sh *.py ./
RUN python -m compileall .

ENTRYPOINT [ "/usr/bin/tickerd", "--", "python" ]

ENV TICKERD_HEALTHCHECK_FILE "/var/run/healthcheck"
HEALTHCHECK --interval=1m --timeout=3s --start-period=3s --retries=1 \
  CMD [ "/usr/bin/tickerd", "-healthcheck" ]
