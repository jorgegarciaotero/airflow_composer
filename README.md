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
- **Workflow:**   It refers to the complete process defined within a DAG, including the execution of tasks, their dependencies, and the overall flow of data or actions.


## DAG Scheduling:
- **start_date**: The timestamp from which the scheduler will attempt to backfill
- **schedule_interval**: How often a DAG runs. Since Airflow 2.4 you use with DAG(Schedule=...) instead.
- **end_date**: The timestamp from which a DAG ends

## Backfilling: 
Backfilling in Airflow refers to the process of running a DAG (Directed Acyclic Graph) for a specific historical period in the past. This allows you to populate your data pipelines with historical data and ensure they are complete
By default, Airflow's catchup parameter determines the behavior when a DAG is scheduled to run daily but hasn't been executed for a while:
- catchup=True (default): The scheduler creates DAG runs for all intervals between the DAG's start_date and the current date, potentially backfilling historical data.
- catchup=False (your current configuration): The scheduler only creates a DAG run for the current date based on the schedule_interval. It won't backfill historical data automatically.

```
With DAG(
    dag_id="10_user_processing",
    start_date=datetime(2024, 3, 23),
    schedule_interval="@daily",
    catchup=True  # Enable backfilling
) as dag:
    # Your DAG tasks here
```

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

## Hooks:
Interfaces that provide a standardized way to interact with external platforms and services. They act as building blocks for operators, which define the actual tasks within your DAGs (Directed Acyclic Graphs). Here's a breakdown of their key aspects:
#### Functionality:
- **Abstraction:** Hooks encapsulate the complexities of interacting with different external APIs, hiding the low-level details and offering a consistent interface for your DAG code.
- **Connection Management:** Hooks handle retrieving connection details from Airflow's connection store, simplifying access to credentials and configuration for external systems.
- **Error Handling:** Hooks can implement custom error handling mechanisms, making your DAGs more robust and easier to maintain.

#### Common Airflow Hooks:
- **Database Hooks:** Interact with various databases like MySQL, PostgreSQL, and SQLite.
- **Cloud Provider Hooks:** Access services offered by cloud platforms like AWS, GCP, and Azure.
- **File System Hooks:** Interact with local and remote file systems.
- **Web Service Hooks:** Communicate with HTTP APIs provided by external services.
- **Email Hooks:** Send email notifications.


## Datasets: 
Logical representation of a group of related data. It acts as a placeholder for data that might be located in various storage systems like files, databases, or cloud object stores. Here's a breakdown of the concept:
#### Purpose:
- **Dependency Management:** Datasets help define data dependencies between different DAGs (Directed Acyclic Graphs).
- **Scheduling:** They enable data-aware scheduling, where a DAG can be triggered only when its dependent datasets are updated.
- **Abstraction:** Datasets hide the underlying storage details, simplifying DAG code and improving maintainability.
#### How Datasets Work:
- **Defining Datasets:** You define a dataset using the Dataset class from Airflow, providing a URI (Uniform Resource Identifier) that specifies the location of the data.
- **Marking Producers and Consumers:**  Tasks that create data (producers) can mark their output as a dataset using the outlet parameter. Similarly, tasks that consume data (consumers) can specify their dependencies on datasets using the schedule_interval parameter set to the dataset itself.
- **Airflow Monitoring:** Airflow monitors the state of datasets.
    - For producer tasks, Airflow updates the dataset metadata when the task finishes successfully.
    - For consumer tasks, Airflow waits until the dependent datasets are updated before triggering the scheduled run.


## Executors:
Executors are the workhorses responsible for running the tasks defined within your DAGs (Directed Acyclic Graphs). They act as a bridge between the Airflow scheduler and the worker processes that execute the actual tasks. Here's a detailed breakdown of their role:

#### Functionality:
- **Task Execution:** Executors receive instructions from the Airflow scheduler about which tasks need to be run.
- **Resource Management:** They allocate resources (like CPU, memory) to worker processes for task execution.
- **Monitoring:** Executors track the status of running tasks (running, succeeded, failed) and report them back to the Airflow scheduler.
- **Scalability:** Airflow allows you to configure multiple executors to distribute task execution across different worker machines, achieving parallelism and improved performance.

#### Types of Executors:
- **Local Executor (Default):** Runs tasks in the same process as the Airflow scheduler. Suitable for small-scale deployments or development environments.
- **Sequential Executor:** Runs tasks sequentially on the same machine as the scheduler. More deterministic but less efficient for parallel processing.
- **Remote Executors:** Orchestrate task execution on separate worker machines. Available options include:
- **Celery Executor:** Leverages Celery, a distributed task queue system, for asynchronous task execution.
- **Kubernetes Executor:** Utilizes Kubernetes clusters for scaling task execution across multiple pods.
- **Dask Executor:** Employs Dask, a parallel computing framework, for distributed task execution.












## Test a Task:
- docker-compose ps:
  
PS C:\proyectos\airflow> docker-compose ps

- docker exec -it <name_scheduler> /bin/bash:

PS C:\proyectos\airflow> docker exec -it airflow-airflow-scheduler-1 /bin/bash

- Use airflow tasks test <dag_id> <task_id> to test a task:

airflow@26482d3211ad:/opt/airflow$ airflow tasks test 01_basic_dag test_python