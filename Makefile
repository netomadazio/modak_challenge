current_branch = 1.0.0

####################################################################################################
####################################################################################################
#####################    COMANDOS PARA CONFIGURAÇÃO DO AMBIENTE    #################################

build_airflow:
	docker build -f docker/airflow/Dockerfile -t airflow:$(current_branch) .

deploy_airflow:
	docker-compose -f services/airflow_services.yaml up -d --build

stop_airflow:
	docker-compose -f services/airflow_services.yaml down

watch_airflow:
	docker-compose -f services/airflow_services.yaml ps