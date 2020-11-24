# Copyright (c) 2020 Bridg Inc. All rights reserved.
# @author Sohil Jain <sohil.jain@bridg.com>

FROM python:3.7-stretch
#WORKDIR ~
WORKDIR /code

RUN apt-get update
RUN apt-get install -y default-jdk
RUN apt-get install -y python3-gnupg
RUN pip install pipenv

COPY Pipfile .
COPY Pipfile.lock .
COPY ./src .
RUN mkdir -p /code/gpghome/

#RUN PIP_USER=1 PIP_IGNORE_INSTALLED=1 pipenv install --skip-lock
RUN pipenv install --skip-lock

ENV PYTHONPATH "${PYTHONPATH}:/code/src"
ENV ALERT_SNS_PARAM="/cdp/SNS/Alarm"

ENTRYPOINT ["pipenv", "run", "python3", "main/pgp_decrypt_upload.py"]
