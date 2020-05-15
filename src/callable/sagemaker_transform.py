import os
from sagemaker.tensorflow.serving import Model

ROLE = os.environ.get("AWS_IAM_ROLE", None)

def run_transform(model_data, input_data, output_path,
                        instance_type, job_name,
                        content_type="application/jsonlines",
                        split_type="Line",
                        assemble_with="Line",
                        strategy="MultiRecord",
                        max_payload=6,
                        framework_version="2.1",
                        ):
    
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
