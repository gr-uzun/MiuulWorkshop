# This Repo was created for the Miuul Workshop Event

## Architecture

![architecture](images/architecture.png)

## Setup Airflow and PostgreSQL with Docker

Note: If Docker is not installed, please visit: [docker.com](https://www.docker.com/products/docker-desktop/)

Start Docker services and open the terminal in the directory where the docker-compose file is located.

Then, run the following commands:

```
docker-compose up -d --build

docker-compose ps

```

Expected output:

![01_docker_compose_ps](images/01_docker_compose_ps.png)

## Airflow UI

Go to [Airflow UI](http://localhost:9090/login/?next=http%3A%2F%2Flocalhost%3A9090%2Fhome)

* Username: airflow
* Password: airflow

![02_airflow_ui](images/02_airflow_ui.png)

## Connect to PostgreSQL Database

docker-compose up postgres

- username: airflow
- password: airflow
- host: localhost
- port: 5433
- database: postgres

_`url: 'postgresql+psycopg2://airflow:airflow@localhost:5433/postgres'`_

## Data Source

* [Uber Pickups in New York City](https://www.kaggle.com/datasets/fivethirtyeight/uber-pickups-in-new-york-city)
* [https://www.kaggle.com/datasets/ravi72munde/uber-lyft-cab-prices](https://www.kaggle.com/datasets/ravi72munde/uber-lyft-cab-prices)