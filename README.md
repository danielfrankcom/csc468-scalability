# csc468-scalability

## Requirements

You must have `docker-compose` installed to run this project.

## Running

Execute `sudo docker-compose up --force-recreate` from the root directory of the project. This will start all of the necessary components for the distributed system.

You can connect to Adminer on `localhost:80`, access the webserver on `localhost:5000`, and manually send POST requests to the transaction server using `localhost:4000`.

### Notes

- You must run `docker compose down` to close down the containers properly. If you do not, next time the containers start up they may be the same instances, and the system will not be in a clean state.
- If you change the docker files, you must run `sudo docker-compose build` to rebuild the images.
- If you want the containers to run in the background, use the `-d` flag at the end of the command.
- If you want to watch a specific container's log files, use `sudo docker-compose logs -f <container name from docker-compose.yml>`.
- If you want to open a shell in a specific container, use `sudo docker-compose -exec <container name from docker-compose.yml> /bin/sh`.
- All commands that start with `docker-compose` must be run from the root directory of the project, or the `docker-compose.yml` file will not be accessible.

## Providing Workloads (Linux/MacOS)

To provide a workload to the transaction server, we need to connect to the workload-generator container. There is a script named `run` in the root directory of the project that can be used as a shortcut for this process.

Execute `./run workloads/<name here>` to pump commands through the system.

If the specified workload contains a `DUMPLOG` command, it will be have been created in `transaction-server/out`. 
