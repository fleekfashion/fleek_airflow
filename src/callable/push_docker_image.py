"""
Script to build and push
docker image to aws ecr
repo
"""
import os
import subprocess
import argparse

import boto3

ROLE = os.environ.get("AWS_IAM_ROLE", None)
REGION = boto3.session.Session().region_name
ACCOUNT_ID = '604924619240'
ACCOUNT_ID = boto3.client('sts').get_caller_identity().get('Account')

def build_repo_uri(ecr_repo: str, tag: str = ':latest'):
    repository_uri = '{}.dkr.ecr.{}.amazonaws.com/{}'.format(ACCOUNT_ID, REGION, ecr_repo + tag)
    return repository_uri

def push_image(ecr_repo: str, dockerfile_dir: str, 
               tag: str = ':latest', **context) -> None:
    repository_uri = build_repo_uri(ecr_repo, tag)
    check_and_create_repo = f"aws ecr describe-repositories --repository-names {ecr_repo} || aws ecr create-repository --repository-name {ecr_repo}" 

    cmds = [
        f"docker-client build {dockerfile_dir} -t {ecr_repo} ",
        f"$(aws ecr get-login --region {REGION} --registry-ids {ACCOUNT_ID} --no-include-email)",
        check_and_create_repo,
        f"docker-client tag {ecr_repo + tag} {repository_uri}",
        f"docker-client push {repository_uri}"
    ]

    ## TODO
    ## Set up docker to run on airflow
    if False:
        for cmd in cmds:
            subprocess.run(cmd, shell=True, 
                check=True, universal_newlines=True)

    context["ti"].xcom_push(key=f"{ecr_repo}_uri", value=repository_uri)

