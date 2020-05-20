import os
from sagemaker.processing import ScriptProcessor

ROLE = os.environ.get("AWS_IAM_ROLE", None)

def run_processing(docker_img_uri: str,
                   processing_filepath: str,
                   inputs: list = None,
                   outputs: list = None,
                   arguments: list = None,
                   instance_type: str = "ml.m5.xlarge",
                    ) -> None:
 
    script_processor = ScriptProcessor(command=['python3'],
                    image_uri=docker_img_uri,
                    role=ROLE,
                    instance_count=1,
                    instance_type=instance_type)

    script_processor.run(code=processing_filepath,
                         inputs=inputs,
                         outputs=outputs,
                         arguments=arguments,
    )
