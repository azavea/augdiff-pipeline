FROM frolvlad/alpine-python3:latest

RUN pip install boto3 polling requests ipdb
COPY poll.py /opt/poll.py

ENTRYPOINT [ "/opt/poll.py" ]
