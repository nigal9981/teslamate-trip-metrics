ARG BUILD_FROM
FROM $BUILD_FROM

RUN apk add --no-cache python3 py3-pip \
  && pip3 install --no-cache-dir --break-system-packages "psycopg[binary]" paho-mqtt

COPY run.sh /run.sh
COPY app.py /app.py

RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
