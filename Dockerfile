# Copyright (c) 2020 Bridg Inc. All rights reserved.
# @author Sohil Jain <sohil.jain@bridg.com>

FROM python:3.7-stretch
RUN mkdir -p /code/gpghome/
#WORKDIR ~
WORKDIR /code

RUN apt-get update
RUN apt-get install -y default-jdk
RUN apt-get install -y python3-gnupg
RUN pip install pipenv

COPY Pipfile .
COPY Pipfile.lock .

#RUN PIP_USER=1 PIP_IGNORE_INSTALLED=1 pipenv install --skip-lock
RUN pipenv install --skip-lock

ENV PYTHONPATH "${PYTHONPATH}:/code/src"
ENV ALERT_SNS_PARAM="/cdp/SNS/Alarm"

COPY ./src .
COPY ./gpghome .
COPY gpghome/1010_decrypt_key.gpg ./gpghome/
COPY gpghome/aurus_decrypt_key.gpg ./gpghome/

ENTRYPOINT ["/code/run.sh"]
