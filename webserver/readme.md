# Flask Web Server

## Running (with Docker)

Docker must be installed on the host machine in order for the below commands to work.

[Download it here](https://docs.docker.com/)

#### Linux / macOS

Both scripts should be executable from any directory.

1. Build the Docker container by executing the `build` script.
2. Run the container by executing the `run` script.

## Running (with Docker Compose)

Docker Compose must be installed on the host machine in order for the below commands to work.

[Download it here](https://docs.docker.com/compose/install/)

#### Linux / macOS

1. Run `sudo docker-compose up`.

## Notes

- If you want to run this container from another directory, use the `-f` flag to specify the `docker-compose.yml` file.
- If you change the `Dockerfile` you will need to use the `--build` flag, or rebuild the container manually.
