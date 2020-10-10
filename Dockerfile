FROM python:3.6

ENV AIRFLOW_HOME /usr/local/airflow
ENV AIRFLOW_USER_ID 9001
ENV SLUGIFY_USES_TEXT_UNIDECODE yes

# Add airflow user
RUN useradd -ms /bin/bash -d ${AIRFLOW_HOME} -u ${AIRFLOW_USER_ID} airflow

RUN apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
    netcat \
    postgresql-client

RUN pip3 install pipenv

# Add Python dependencies
RUN cd $AIRFLOW_HOME
ADD Pipfile Pipfile
ADD Pipfile.lock Pipfile.lock
RUN pipenv install --dev --system --deploy

# Cleanup any unneeded temp files
RUN apt-get clean \
    && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/* \
    /usr/share/man \
    /usr/share/doc \
    /usr/share/doc-base

# Create directory to store dags and logs
RUN mkdir ${AIRFLOW_HOME}/dags \
    && mkdir ${AIRFLOW_HOME}/logs

# Grab all dags, configs, and scripts needed
ADD dev/docker/database_setup.sh ${AIRFLOW_HOME}/database_setup.sh 
ADD dev/docker/dbfs_configure.sh ${AIRFLOW_HOME}/dbfs_configure.sh 
ADD dev/docker/entrypoint.sh ${AIRFLOW_HOME}/entrypoint.sh
ADD dev/docker/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

RUN \
    chown -R airflow: ${AIRFLOW_HOME} \
    && chmod +x ${AIRFLOW_HOME}/entrypoint.sh

EXPOSE 8888 8889 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["./entrypoint.sh"]
