FROM python:3.9-alpine

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN apk add tzdata && \
    pip3 install --no-cache-dir  -r requirements.txt

ENV FROST_SERVER ""
ENV FROST_USER ""
ENV FROST_PASSWORD ""

ENV CAMDATA_URL ""

ENV TZ "Europe/Berlin"

COPY src/ ./

CMD [ "python", "-u", "thermiCam_import.py"]
