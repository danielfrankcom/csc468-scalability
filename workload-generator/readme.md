# Workload Generator

## Running

Docker Compose must be installed on the host machine in order for the below commands to work.

[Download it here](https://docs.docker.com/compose/install/)

#### Linux / macOS

1. Run `sudo docker-compose up`.
2. In another terminal, execute the `run` script and specify the workload file as the first argument.

## Notes

- If you want to run the workload-generator `docker-compose.yml`, you *cannot* also run the webserver `docker-compose.yml`.
- If you want to run the containers from another directory, use the `-f` flag to specify the `docker-compose.yml` file.
- If you change either `Dockerfile` you will need to use the `--build` flag.

