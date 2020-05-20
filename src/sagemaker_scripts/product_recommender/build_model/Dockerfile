FROM python:3.7

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt 
RUN apt-get install libglib2.0-0
ENV PYTHONUNBUFFERED=TRUE

COPY ./gcloud/ /root/.config/gcloud 
ENV GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json

ENTRYPOINT ["python3"]
