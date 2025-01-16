# t5-commons
Code required for the Taskforce5 commons project.

## Instructions for building and deploying teh genome browser at SIBYLS

These instructions are only really useful for the particular setup at the SIBYLS beamline. I keep forgetting what steps to perform when changes have been made to the app.

On `hyperion`

clone the repository

```bash
git clone git@github.com:lbl-cbg/t5-commons.git
```

Build all service Docker images

```bash
cd t5-commons/BRAVE
docker compose -f docker-compose.prod.yml  -p brave build
```

Start (and daemonize) the Docker services

```bash
docker compose -f docker-compose.prod.yml  -p brave up -d
```

If you need to stop the app

```bash
docker compose -f docker-compose.prod.yml  -p brave down
```

Check status of services

```bash
docker compose -p brave ps

NAME          IMAGE                  COMMAND                  SERVICE   CREATED          STATUS          PORTS
db-prod       postgres:16.1-alpine   "docker-entrypoint.s…"   db        15 minutes ago   Up 15 minutes   0.0.0.0:5432->5432/tcp, :::5432->5432/tcp
django-prod   brave-django           "/srv/app/entrypoint…"   django    15 minutes ago   Up 15 minutes   0.0.0.0:8001->8000/tcp, :::8001->8000/tcp
nginx-prod    brave-nginx            "/docker-entrypoint.…"   nginx     15 minutes ago   Up 15 minutes   0.0.0.0:8080->80/tcp, :::8080->80/tcp
react-prod    brave-react            "docker-entrypoint.s…"   react     15 minutes ago   Up 15 minutes   0.0.0.0:3000->3000/tcp, :::3000->3000/tcp
```
