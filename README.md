# Apache Airflow

Note: To be able to work with these examples, install docker desktop, create the .env variable and then open Docker Desktop and execute "docker-compose up -d"

## Core Components:

- **Web server:** Provides a user interface for Airflow. It allows users to monitor DAGs and task statuses, trigger DAG runs manually, view logs and code for tasks and manage users and connections.

- **Scheduler:** It is responsible for periodically scanning DAG definitions, identifying DAGs that are ready to run based on their schedule intervals or dependencies and triggering the execution of tasks within those DAGs by sending them to the executor.

- **Metastore:** Acts as a central repository that stores information about Airflow's workflows. This includes details like DAG definitions (tasks, dependencies, configurations), DAG run details (start/end times, task statuses, logs), Variable values and connections or Information about users and their roles.

- **Triggerer:**  In some Airflow versions, the triggerer acts as a separate component responsible for monitoring external events or signals and triggering DAG runs based on these external events (e.g., file arrival, sensor completion).

- **Executor:** The executor receives tasks from the scheduler and is responsible for Selecting a worker to run the task, Submitting the task to the worker for execution and Monitoring the task's execution and reporting its status back to the scheduler.

- **Queue:** An optional queue can be used as a buffer between the scheduler and the executor. Tasks are placed in the queue by the scheduler. Workers pull tasks from the queue for execution. This can be helpful for managing load and ensuring worker availability.

- **Worker:**  Workers are processes or containers that actually execute the tasks defined in DAGs. They receive tasks from the executor, execute the defined task logic (e.g., python functions, shell commands) and report the task's completion status (success, failure) back to the executor.

## Core Concepts:

- **DAG (Directed Acyclic Graph):** It is the fundamental unit of work in Airflow. It represents a directed workflow made up of tasks with well-defined dependencies.defines the overall orchestration of your data pipelines or workflows.

- **Operator:** Defines a specific action to be performed within a DAG. It provides the building blocks for your workflows. Airflow comes with a rich set of built-in operators for various tasks like:

- **Workflow: **   It refers to the complete process defined within a DAG, including the execution of tasks, their dependencies, and the overall flow of data or actions.


## Types of Operators:
- **Action Operators:** Execute an action.
- **Transfer Operators:** Transfer data.
- **Sensors:** Wait for a condition to be met.

## Providers
Providers are essentially plugins that extend its capabilities. They allow Airflow to interact with various external services, data sources, and systems.
#### Functionality:
- **Connection Types:** Providers introduce new connection types for connecting Airflow to external services. These connections store credentials and other details needed for communication.
- **Operators:** Providers offer new operators specifically designed to interact with the external service. These operators handle tasks like data transfer, triggering actions, or monitoring the service.
- **Hooks:** Providers might also include hooks that provide lower-level access to the service's functionalities.
- **Sensors:** Some providers offer sensors that allow Airflow to wait for specific conditions within the external service before proceeding with downstream tasks.

#### Benefits of Providers:
- **Modular Design:** Providers allow for a modular Airflow installation. You can install only the providers you need for your specific use cases.
- **Extensibility:** The provider ecosystem allows Airflow to integrate with a wide range of technologies.
- **Community-Driven:** Many providers are developed and maintained by the Airflow community, offering a vast selection of integrations.

#### Types of Providers:

- **Officially Supported Providers:** These are providers maintained by the Apache Airflow project itself. They cover popular services like Amazon S3, Google Cloud Storage, MySQL, and many more.
- **Community-Developed Providers:** A large number of providers are developed and maintained by the Airflow community. You can find them on platforms like PyPI (Python Package Index).

#### Install Providers: 
- Use pip install apache-airflow-providers-<service_name> to install the provider for a specific service (e.g., apache-airflow-providers-amazon).
- Import Operators and Hooks: Import the operators and hooks you need from the provider package in your DAGs.
- Use Operators and Connections: Utilize the operators and connection definitions within your DAGs to interact with the external service.




#### Test a Task:
- docker-compose ps:
PS C:\proyectos\airflow> docker-compose ps
time="2024-03-24T16:14:40+01:00" level=warning msg="The \"AIRFLOW_UID\" variable is not set. Defaulting to a blank string."
time="2024-03-24T16:14:40+01:00" level=warning msg="The \"AIRFLOW_UID\" variable is not set. Defaulting to a blank string."
NAME                          IMAGE                  COMMAND                  SERVICE             CREATED          STATUS                   PORTS
airflow-airflow-scheduler-1   apache/airflow:2.4.2   "/usr/bin/dumb-init …"   airflow-scheduler   45 minutes ago   Up 6 minutes (healthy)   8080/tcp
airflow-airflow-triggerer-1   apache/airflow:2.4.2   "/usr/bin/dumb-init …"   airflow-triggerer   45 minutes ago   Up 6 minutes (healthy)   8080/tcp
airflow-airflow-webserver-1   apache/airflow:2.4.2   "/usr/bin/dumb-init …"   airflow-webserver   45 minutes ago   Up 6 minutes (healthy)   0.0.0.0:8080->8080/tcp
airflow-airflow-worker-1      apache/airflow:2.4.2   "/usr/bin/dumb-init …"   airflow-worker      45 minutes ago   Up 6 minutes (healthy)   8080/tcp
airflow-postgres-1            postgres:13            "docker-entrypoint.s…"   postgres            45 minutes ago   Up 6 minutes (healthy)   5432/tcp
airflow-redis-1               redis:latest           "docker-entrypoint.s…"   redis               45 minutes ago   Up 6 minutes (healthy)   6379/tcp

- docker exec -it <name_scheduler> /bin/bash
PS C:\proyectos\airflow> docker exec -it airflow-airflow-scheduler-1 /bin/bash

- Use airflow tasks test <dag_id> <task_id> to test a task:
airflow@26482d3211ad:/opt/airflow$ airflow tasks test 01_basic_dag test_python