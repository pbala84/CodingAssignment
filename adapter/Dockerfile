FROM python:3.7-stretch

RUN pip install requests pymongo pytz

COPY ./main.py /main.py
COPY ./config.py /config.py

ENTRYPOINT python3 /main.py
