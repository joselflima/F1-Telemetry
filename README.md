![Image](./docs/F1%20Pipeline-2.png "Pipeline architecture")

# F1 Telemetry: From Fast Laps to Fast Decisions

## About The Project
This project is a comprehensive data engineering pipeline designed to capture, process, and visualize real-time telemetry data from the F1 23 game. It was developed as a final project for a graduation course and serves as a practical demonstration of building a robust, scalable, and fault-tolerant data pipeline using modern, open-source technologies. The system simulates the high-pressure environment of Formula 1, where real-time data analysis is crucial for making strategic decisions that can change the outcome of a race.

## Core Concepts

- **Real-Time Data Ingestion**: The pipeline starts by capturing telemetry data broadcasted by the F1 23 game over a UDP network. A high-performance Go application listens for these packets, decodes them, and initiates the data flow.

- **Scalable Data Streaming**: At the heart of the architecture is Apache Kafka, a distributed streaming platform that decouples data producers from consumers. This ensures that the data pipeline is resilient and can handle high-velocity data streams without loss.

- **Dual Data Processing Paths**: The architecture implements two distinct data processing paths:

    - **Hot Path (Real-Time Dashboard)**: A Go-based Kafka consumer processes telemetry data as it arrives, feeding it into InfluxDB, a time-series database optimized for high-write and query loads. This path powers a real-time dashboard that visualizes key performance indicators.

   - **Cold Path (Post-Race Analysis)**: A Python-based Kafka consumer saves the raw telemetry data to a MinIO data lake. After the "race" is over, a separate ETL (Extract, Transform, Load) job processes this data and loads it into a PostgreSQL data warehouse for in-depth, post-event analysis.

- **Containerization**: The entire application is containerized using Docker and managed with Docker Compose, allowing for easy setup, deployment, and scaling.

## Getting Started
To get this project up and running, follow these simple steps.

### Prerequisites

- Docker/Podman

- Docker Compose/Podman Compose

### Installation

1. Clone the repo:

```bash
git clone https://github.com/zarkuslol/f1-telemetry.git
```

2. Navigate to the project directory:

```bash
cd f1-telemetry/F1-Telemetry-main
```

### Running the Application

1. **Start all services**:

This command will build the Docker images and start all the services defined in the ```docker-compose.yml``` file, including the data collector, Kafka, InfluxDB, PostgreSQL, MinIO, and the real-time consumers.

- Docker:
```bash
docker-compose up --build
```

- Podman:
```bash
podman compose --file docker-compose.yml up --build
```

2. **Run the post-race ETL process**:

After you have finished your "race" and data has been collected in the MinIO data lake, run the following command to trigger the ETL job. This will process the data from MinIO and load it into the PostgreSQL data warehouse for further analysis.

- Docker:
```bash
docker-compose run --rm etl-job
```

- Podman:
```bash
podman compose --file docker-compose.yml up --detach
```

## Project Structure
- ```docker-compose.yml```: Defines and configures all the services of the application.

- ```src/```: Contains the source code for the Go and Python applications.

    - ```main.go```: The main entry point for the Go data collector.

    - ```messaging/```: Includes the Kafka producers and consumers.

    - ```telemetry/```:  Contains the logic for decoding and processing the F1 23 telemetry packets.

    - ```scripts/```: Holds the Python script for the post-race ETL job.

- ```dockerfiles/```: Contains the Dockerfiles for building the application's services.

- ```template/```: Includes a LaTeX template and a PDF of the final project report ("TCC").

# License
This project is licensed under the MIT License - see the LICENSE file for details.