FROM quay.io/azavea/hive:latest

COPY adi-update.sh /opt/adi-update.sh

ENTRYPOINT [ "/opt/adi-update.sh" ]
