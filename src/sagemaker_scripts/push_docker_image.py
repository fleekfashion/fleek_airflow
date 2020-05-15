import os
import subprocess
import argparse

import boto3
from botocore.exceptions import ClientError

ROLE = os.environ.get("AWS_IAM_ROLE", None)
REGION = boto3.session.Session().region_name
ACCOUNT_ID = '604924619240'
ACCOUNT_ID = boto3.client('sts').get_caller_identity().get('Account')

def build_repo_uri(ecr_repo, tag=':latest'):
    repository_uri = '{}.dkr.ecr.{}.amazonaws.com/{}'.format(ACCOUNT_ID, REGION, ecr_repo + tag)
    return repository_uri

def push_image(ecr_repo, dockerfile_dir, tag=':latest', **context):
    repository_uri = build_repo_uri(ecr_repo, tag)
    print("URI:", repository_uri)

    check_and_create_repo = f"aws ecr describe-repositories --repository-names {ecr_repo} || aws ecr create-repository --repository-name {ecr_repo}" 

    cmds = [
        f"docker build {dockerfile_dir} -t {ecr_repo} ",
        f"$(aws ecr get-login --region {REGION} --registry-ids {ACCOUNT_ID} --no-include-email)",
        check_and_create_repo,
        f"docker tag {ecr_repo + tag} {repository_uri}",
        f"docker push {repository_uri}"
    ]

    ## TODO
    ## Set up docker to run on airflow
    if True:
        for cmd in cmds:
            subprocess.run(cmd, shell=True, 
                check=True, universal_newlines=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=str, required=True)
    parser.add_argument("--ecr_repo", type=str, required=True)
    args = parser.parse_args()
    push_image(args.ecr_repo, args.path)
