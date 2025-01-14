<h1 align="center"> Modak Data Challenge</h1>

## Repository created for the development of the proposed exercise as part of the evaluation process for Modak

<h4 align="center"> 
	Modak Challenge Project 🚀
</h4>

## Contents 

* [Title](#title)
* [Contents](#contents)
* [Project Description](#project-description)
* [Technologies Used](#technologies-used)
* [Solution](#solution)
* [Points of Improvement](#points-of-improvement)
* [Project Execution](#project-execution)
* [Conclusion](#conclusion)
* [Author](#author)

## Project Description

Este projeto visa a resolução de um case proposto pela empresa Modak como parte de um processo avaliativo. O objetivo é realizar uma análise detalhada e encontrar discrepâncias e ou padrões nas tabelas de backend utilizadas para pagamentos. 

## Technologies Used
:hammer:

- `Github`: Tool used for project versioning and repository management;
- `Python`: Language utilized for manipulating files and necessary data;
- `Docker`: Responsible for creating the required image to process the application;
- `Docker-compose`: Manages the orchestration of containers;
- `Airflow`: Organizes and coordinates the workflow.

## Solution

The first step in developing the project was to define the approach for analysis, identify patterns and discrepancies in the data, select the appropriate technologies, and determine the necessary aspects to meet the requirements of the proposed case. With this in mind, the case resolution was divided into two main stages.

The first stage involved creating a DAG using Airflow as the orchestration tool. The goal was to implement a mechanism to validate the main rules related to the backend tables, enabling the identification of inconsistencies in the data. With this DAG, it is possible to perform daily data checks, prevent incorrect payments, and allow the backend team to identify issues in a timely manner.

- Orchestration Tool:
  Airflow was chosen due to its robustness in task orchestration. The tool provides features such as task scheduling, dependency management, error monitoring, retry configuration, and a user-friendly interface to track ongoing processes.

- Language:
  Python was used in the development of the case due to its excellent capabilities for data manipulation and its compatibility with Airflow, ensuring efficient integration.

<div style="color:red;">
    IMPORTANT NOTE IN PROJECT DEVELOPMENT

  During the tests, it was observed that the data had low volume. As a result, pandas was used to perform the necessary transformations, as there was no need to employ a distributed processing tool like Spark.

</div>

- Containerization:
  Docker remains a relevant choice for containerization due to its widespread use and seamless integration with various projects. It enables modularization and ensures application portability by creating a package with all its dependencies, allowing it to run both locally and in cloud environments.

With the solution designed, the case resolution process began. Below is a step-by-step description of the approach:

- Task 1 (init_process_authentication):
  This task was created to symbolize the common requirement in corporate environments (whether on-premises or in cloud services) for authentication to access data lakes, databases, and other resources for reading and writing purposes.

- Task 2 (look_for_inconsistencies):
  This task is designated to evaluate the correlation between data sources. Considering the "allowance_events" table as the source of truth, inconsistencies in the "allowance_backend" table were identified, particularly in the "frequency" and "day" fields. Additionally, it analyzed disabled users in the "payment_schedule_backend" table and validated the consistency between the "next_payment_day" and "payment_date" fields.

- Task 3 (look_allowance_backend):
  This task focused on validating the "allowance_backend" table, identifying null values, data duplication, and unexpected values in specific columns.

- Task 4 (look_payment_schedule):
  Similar to Task 3, this task performed validations on the "payment_schedule_backend" table, identifying possible data inconsistencies.

Within the tasks, validations, error handling, and exception management were implemented. These events can be viewed in the Airflow logs, with the option to re-execute tasks in case of failures.

If the tasks encounter errors, it would indicate the existence of inconsistencies in the data, identified through validation tests, or an error in the pipeline processing. To make the process more complete and robust, a good implementation would be to automate the sending of emails or even a message in a corporate chat, notifying the need to verify the data.

The second stage of the approach involved an exploratory data analysis. This allowed for a deeper investigation of the data to identify inconsistencies, patterns, behaviors, and potential causes. A notebook was used to create a report containing the analyses performed, insights gathered, relevant points, and observations. The file is available in the repository and can be re-executed, if necessary, to include new analyses and findings.

## Improvement Points

Several improvement points can be highlighted for the development of the project:

Unit Tests: Implementing unit tests for the developed code would allow for validation of the integrity of the applications, ensuring that the expected behavior is maintained over time.

Great Expectations: In a real scenario, it would be beneficial to implement data validations based on business rules established by stakeholders, ensuring data quality.

Power BI: Creating automated reports in Power BI, which incorporate the data validations, would provide a clearer and more interactive view of the results and identified inconsistencies.

Email: Implementing an email notification system for data inconsistencies would alert the responsible teams, enabling them to act quickly and effectively to resolve issues.


## Project Execution
📁 

To execute the project fully, follow the steps described below:

  - Clone the project repository:
	```sh
	git clone https://github.com/netomadazio/modak_challenge.git

  - Check the current branch:
	```sh
	git status

  - If not on the main branch, switch to the main branch:
    ```sh
    git checkout main

  - Grant permissions for Airflow on the directories:
  -		
	```sh
	sudo chmod -R 777 ./mnt/airflow/logs
  -    
	```sh
	sudo chmod -R 777 ./mnt/airflow/dags
  -  
	```sh
	sudo chmod -R 777 ./mnt/airflow/plugins

After completing these steps, you will need to create the images used in the project execution:

  - Use the Makefile to execute the following commands to instantiate the infrastructure::
    - Build the Airflow image:
      ```sh
      make build_airflow

  - Instantiate the Airflow environments:
    - Run the Airflow environment:
      ```sh
      make deploy_airflow
   
    Pode-se verficar se o ambiente foi provisionados através dos comandos:
    - Verificar ambiente Airflow:
      ```sh
      make watch_airflow
  

After ensuring the environment is running, you can access the Airflow interface at http://localhost:8080/. There, you can run the DAG and monitor the execution logs.

Deactivate the environments after executions:
  - Stop the Airflow environment:
    ```sh
    make stop_airflow

Process Finished.

## Conclusion

The proposed work allows the development of a complete ETL pipeline using widely available tools in the market. It is important to note that there are opportunities for improvement in the steps performed, which could lead to a more robust development, optimized execution, and higher quality deliverables.
The main goal of creating this mechanism is to ensure continuous monitoring of data consistency, which helps maintain smooth operational processes by preventing inconsistencies and failures.
I appreciate the opportunity to work on this project and remain available for any questions or clarifications.

Thank you very much, Modak

Best Regards,

### Author

Irineu Madazio Neto,
Control and Automation Engineer,
Sênior Data Engineer,
Passionate about Data Engineering.

[![Linkedin Badge](https://img.shields.io/badge/-Irineu-blue?style=flat-square&logo=Linkedin&logoColor=white&link=https://www.linkedin.com/in/irineu-madazio-neto/)](https://www.linkedin.com/in/irineu-madazio-neto/) 
