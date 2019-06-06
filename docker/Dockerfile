FROM python:3.5.7-slim-stretch

WORKDIR /root/

ADD requirements.txt /root/requirements.txt
RUN pip install -r requirements.txt --proxy=127.0.0.1:12333

ADD  ./add /root/
RUN mkdir log
RUN python superset_stat.py &
CMD python start.py

