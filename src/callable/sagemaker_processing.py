import os
from sagemaker.processing import ScriptProcessor, ProcessingInput, ProcessingOutput

ROLE = os.environ.get("AWS_IAM_ROLE", None)

def run_processing(docker_img_uri,
                   processing_filepath,
                   s3_input_dir=None, input_filename=None,
                   s3_output_dir=None, output_filename=None,
                   pid_output_filename=None,
                   instance_type="ml.m5.xlarge",
                    ):
    import boto3
    REGION = boto3.session.Session().region_name
    print("REGION", REGION)

    arguments = []
    if s3_input_dir is not None:
        input_source = os.path.join([s3_input_dir, input_filename])
        system_input_dir = '/opt/ml/processing/input/'
        inputs = [ProcessingInput(source=input_source,
                                  destination=system_input_dir,
                                )]

        arguments.append(f"--input_path={input_source+input_filename}")
    else:
        inputs = None

    
    if s3_output_dir is not None:
        output_source = '/opt/ml/processing/output/'
        outputs = [ProcessingOutput(destination=s3_output_dir,
                                    source=output_source,
                       )]
        arguments.append(f"--pid_out={output_source+pid_output_filename}")
        arguments.append(f"--main_out={output_source+output_filename}")
    else:
        outputs = None
        
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
