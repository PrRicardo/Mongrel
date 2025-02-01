import subprocess
import time
import logging
import warnings

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DockerManagement:

    @staticmethod
    def run_docker_command(command) -> str:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logger.debug("Running docker command: {}".format(command))
        if result.returncode != 0:
            raise RuntimeError(f"Error running command: {' '.join(command)}\n{result.stderr}")
        return result.stdout

    @staticmethod
    def create_network(network_name):
        DockerManagement.run_docker_command(["docker", "network", "create", network_name])

    @staticmethod
    def remove_network(network_name):
        DockerManagement.run_docker_command(["docker", "network", "rm", network_name])

    @staticmethod
    def container_exists(container_name) -> bool:
        output = DockerManagement.run_docker_command(
            ["docker", "ps", "-a", "--filter", f"name={container_name}", "--format", "{{.Names}}"])
        return container_name in output.split('\n')

    @staticmethod
    def container_is_running(container_name) -> bool:
        output = DockerManagement.run_docker_command(
            ["docker", "ps", "--filter", f"name={container_name}", "--format", "{{.Names}}"])
        return container_name in output.split('\n')

    @staticmethod
    def network_exists(network_name):
        result = DockerManagement.run_docker_command(
            ["docker", "network", "ls", "--filter", f"name={network_name}", "--format", "{{.Name}}"])
        return network_name in result.splitlines()

    @staticmethod
    def run_postgres_container(container_name, network_name, db_password, db_port: int = 5432):
        DockerManagement.run_docker_command([
            "docker", "run", "--name", container_name,
            "-e", f"POSTGRES_PASSWORD={db_password}",
            "-p", f"{db_port}:5432",
            "--network", network_name, "-d", "postgres"
        ])

    @staticmethod
    def run_mongodb_container(container_name, network_name, db_password, db_user="mongo", db_port: int = 27017):
        DockerManagement.run_docker_command([
            "docker", "run", "--name", container_name,
            f"--env=MONGO_INITDB_ROOT_USERNAME={db_user}",
            f"--env=MONGO_INITDB_ROOT_PASSWORD={db_password}",
            "-p", f"{db_port}:27017",
            "--network", network_name, "-d", "mongo"
        ])

    @staticmethod
    def stop_and_remove_container(container_name):
        if DockerManagement.container_is_running(container_name):
            DockerManagement.run_docker_command(["docker", "stop", container_name])
        if DockerManagement.container_exists(container_name):
            DockerManagement.run_docker_command(["docker", "rm", container_name])

    @staticmethod
    def create_database_server(network_name: str, container_name: str, db_password: str, db_port: int = 5432):
        """
        Deprecated method. Use create_postgres_server instead.
        """
        warnings.warn(
            "The 'create_database_server' method is deprecated and will be removed in a future release. "
            "Use 'create_postgres_server' instead.",
            DeprecationWarning
        )
        DockerManagement.create_postgres_server(network_name, container_name, db_password, db_port)

    @staticmethod
    def create_postgres_server(network_name: str, container_name: str, db_password: str, db_port: int = 5432):
        logger.info(f"CREATE docker network {network_name} and postgres-container {container_name}!")
        if DockerManagement.network_exists(network_name):
            raise FileExistsError("Docker network already exists, abort testing!")
        DockerManagement.create_network(network_name)
        DockerManagement.run_postgres_container(container_name, network_name, db_password, db_port)
        # Wait for PostgreSQL to start
        time.sleep(10)
        return

    @staticmethod
    def create_mongodb_server(network_name: str, container_name: str, db_password: str, db_port: int = 27017):
        logger.info(f"Setting up MongoDB server '{container_name}' on network '{network_name}'")
        if DockerManagement.network_exists(network_name):
            raise FileExistsError(f"Docker network '{network_name}' already exists. Aborting.")
        DockerManagement.create_network(network_name)
        DockerManagement.run_mongodb_container(container_name=container_name, network_name=network_name,
                                               db_port=db_port, db_password=db_password)
        # Wait for MongoDB to start
        time.sleep(10)
        logger.info(f"MongoDB server '{container_name}' is up and running.")

    @staticmethod
    def remove_database_server(network_name: str, container_name: str):
        logger.info(f"REMOVE docker network {network_name} and postgres-container {container_name}!")
        DockerManagement.stop_and_remove_container(container_name)
        DockerManagement.remove_network(network_name)


if __name__ == "__main__":
    DockerManagement.create_mongodb_server("MongoNetwork", "MongoContainer")
    DockerManagement.remove_database_server("MongoNetwork", "MongoContainer")
