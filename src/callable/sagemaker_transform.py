"""
Run sagemaker batch
batch transformation job.
"""
import os
from sagemaker.tensorflow.serving import Model

ROLE = os.environ.get("AWS_IAM_ROLE", None)

def run_transform(model_data: str, input_data: str, output_path: str,
                  instance_type: str, job_name: str,
                  content_type: str = "application/jsonlines",
                  split_type: str = "Line",
                  assemble_with: str = "Line",
                  strategy: str = "MultiRecord",
                  max_payload: int = 6,
                  framework_version: str = "2.1",
                  ) -> None:
   
    model = Model(model_data=model_data,
                  role=ROLE, framework_version=framework_version)

    transformer = model.transformer(instance_count=1,
                                    instance_type=instance_type,
                                    output_path=output_path,
                                    strategy=strategy,
                                    max_payload=max_payload,
                                    assemble_with=assemble_with,
                                   )
    transformer.transform(data=input_data,
                          content_type=content_type,
                          split_type=split_type,
                          job_name=job_name,
                          wait=True,
                          logs=True
                          )
    print("Great Success")
