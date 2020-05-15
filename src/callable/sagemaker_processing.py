import os
from sagemaker.processing import ScriptProcessor, ProcessingInput, ProcessingOutput

ROLE = os.environ.get("AWS_IAM_ROLE", None)

def run_processing(docker_img_uri,
                   processing_filepath,
                   inputs=None,
                   outputs=None,
                   arguments=None,
                   instance_type="ml.m5.xlarge",
                    ):
        
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
