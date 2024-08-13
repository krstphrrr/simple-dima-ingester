import cmd
import docker
import os
import logging

from _2_dima_loadingest.scripts.utils import process_csv
from _2_dima_loadingest.config import DOCKERFILE_DIR, DATA_DIR

logger = logging.getLogger(__name__)

class DockerCLI(cmd.Cmd):
    intro = 'Welcome to the Docker CLI. Type help or ? to list commands.\n'
    prompt = '(docker-cli) '

    def __init__(self):
        super().__init__()
        self.docker_client = docker.from_env()

    def do_extract(self, arg):
        'Extract tables specified instide /_1_dima_extract/export.sh into /extracted'

        dockerfile_directory = DOCKERFILE_DIR
        image_tag = "test:latest"
        output_directory = DATA_DIR

        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        else:
            # Check if the output directory is empty
            if os.listdir(output_directory):
                print(f"Warning: The output directory '{output_directory}' is not empty.")
                user_input = input("Do you want to clear the directory before proceeding? (y/n): ")
                if user_input.lower() == 'y':
                    # Clear the directory
                    for file in os.listdir(output_directory):
                        file_path = os.path.join(output_directory, file)
                        try:
                            if os.path.isfile(file_path) or os.path.islink(file_path):
                                os.unlink(file_path)
                            elif os.path.isdir(file_path):
                                os.rmdir(file_path)
                        except Exception as e:
                            print(f"Failed to delete {file_path}. Reason: {e}")
                else:
                    print("Proceeding without clearing the directory.")

        try:
            print("Building the Docker image...")
            image, logs = self.docker_client.images.build(path=dockerfile_directory, tag=image_tag)
            for log in logs:
                if 'stream' in log:
                    print(log['stream'].strip())

            print(f"Image '{image_tag}' built successfully.")

            print("Running the container...")
            container = self.docker_client.containers.run(
                image_tag,
                detach=True,
                volumes={
                    os.path.abspath(output_directory): {'bind': '/extracted', 'mode': 'rw'}
                }
            )

            # Waiting for the container to complete the process
            result = container.wait()
            print(f"Container finished with exit code {result['StatusCode']}")
            logs = container.logs().decode('utf-8')
            print("Container logs:\n", logs)

        except docker.errors.BuildError as e:
            print(f"Build failed: {e}")
        except docker.errors.APIError as e:
            print(f"Docker API error: {e}")
        finally:
            # Ensure the container is stopped and removed
            container.remove(force=True)
            print("Container stopped and removed.")

    def do_ingest(self, arg):
        data_dir = DATA_DIR

        for file_name in os.listdir(data_dir):
            file_path = os.path.join(data_dir, file_name)

            # Check if the file is a CSV
            if file_name.endswith(".csv"):
                process_csv(file_name, file_path)
            else:
                logger.info(f"Skipping non-CSV file: {file_name}")


    def do_exit(self, arg):
        'Exit the CLI'
        print('Exiting the CLI.')
        return True

if __name__ == '__main__':
    DockerCLI().cmdloop()
