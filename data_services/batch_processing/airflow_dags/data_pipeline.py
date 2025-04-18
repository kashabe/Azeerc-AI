# data_pipeline.py
from datetime import datetime, timedelta
from typing import Dict, List

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s_models

DAG_CONFIG = {
    "dag_id": "azeerc_batch_pipeline",
    "description": "Enterprise batch data processing pipeline",
    "schedule_interval": "@daily",
    "start_date": datetime(2023, 8, 1),
    "max_active_runs": 1,
    "default_args": {
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "on_failure_callback": None,  # Set via init
    },
    "tags": ["production", "batch", "etl"],
    "catchup": False,
    "doc_md": """## Azeerc AI Batch Pipeline
Main processing workflow for:
- Data ingestion
- ETL processing
- ML model updates
- Compliance checks"""
}

class AzeercDataPipeline:
    def __init__(self):
        self.dag = self._init_dag()
        self._setup_tasks()
        
    def _init_dag(self):
        return DAG(
            dag_id=DAG_CONFIG["dag_id"],
            description=DAG_CONFIG["description"],
            schedule_interval=DAG_CONFIG["schedule_interval"],
            start_date=DAG_CONFIG["start_date"],
            max_active_runs=DAG_CONFIG["max_active_runs"],
            default_args=self._get_default_args(),
            tags=DAG_CONFIG["tags"],
            catchup=DAG_CONFIG["catchup"],
            doc_md=DAG_CONFIG["doc_md"],
            params={
                "processing_date": "{{ ds }}",
                "environment": Variable.get("DEPLOY_ENV", default_var="dev")
            }
        )

    def _get_default_args(self):
        return {
            **DAG_CONFIG["default_args"],
            "on_failure_callback": self._alert_failure,
            "on_success_callback": self._alert_success
        }

    def _setup_tasks(self):
        # ===== Pipeline Steps =====
        start = DummyOperator(task_id="start", dag=self.dag)
        
        validate_env = BranchPythonOperator(
            task_id="validate_environment",
            python_callable=self._check_environment,
            dag=self.dag
        )

        dev_branch = DummyOperator(task_id="dev_skip_checks", dag=self.dag)
        
        prod_checks = ExternalTaskSensor(
            task_id="wait_for_upstream",
            external_dag_id="azeerc_ingestion_pipeline",
            external_task_id="complete",
            execution_delta=timedelta(hours=1),
            timeout=3600,
            mode="reschedule",
            dag=self.dag
        )

        data_validation = SparkSubmitOperator(
            task_id="run_data_validation",
            application="/opt/airflow/dags/src/spark/validate_job.py",
            conn_id="spark_k8s",
            conf={
                "spark.kubernetes.namespace": "airflow",
                "spark.kubernetes.authenticate.driver.serviceAccountName": "airflow",
            },
            dag=self.dag
        )

        etl_process = KubernetesPodOperator(
            task_id="run_etl_process",
            name="etl-process",
            image="azeerc/spark:3.3.1",
            cmds=["/opt/run_etl.sh"],
            env_vars={
                "PROCESSING_DATE": "{{ params.processing_date }}",
                "ENVIRONMENT": "{{ params.environment }}"
            },
            secrets=[
                k8s_models.V1EnvVar(
                    name="DB_CREDENTIALS",
                    value_from=k8s_models.V1EnvVarSource(
                        secret_key_ref=k8s_models.V1SecretKeySelector(
                            name="database-secrets",
                            key="connection-string"
                        )
                    )
                )
            ],
            dag=self.dag
        )

        compliance_check = PythonOperator(
            task_id="run_compliance_checks",
            python_callable=self._execute_compliance_check,
            dag=self.dag
        )

        end = DummyOperator(
            task_id="complete",
            trigger_rule=TriggerRule.NONE_FAILED,
            dag=self.dag
        )

        # ===== Task Dependencies =====
        start >> validate_env
        validate_env >> [dev_branch, prod_checks]
        prod_checks >> data_validation
        data_validation >> etl_process
        etl_process >> compliance_check
        compliance_check >> end
        dev_branch >> end

    def _check_environment(self, **context) -> str:
        env = context["params"]["environment"]
        return "dev_skip_checks" if env == "dev" else "wait_for_upstream"

    def _execute_compliance_check(self, **context):
        # Implementation using a compliance library
        from azeerc_compliance import GDPRValidator
        validator = GDPRValidator(
            execution_date=context["execution_date"],
            environment=context["params"]["environment"]
        )
        return validator.run_checks()

    @staticmethod
    def _alert_failure(context):
        slack_msg = f"""
        :red_circle: Pipeline Failed. 
        *Task*: {context.get('task_instance').task_id}  
        *Dag*: {context.get('dag').dag_id} 
        *Execution Time*: {context.get('execution_date')}  
        *Log*: {context.get('task_instance').log_url}
        """
        SlackWebhookOperator(
            task_id="slack_alert",
            http_conn_id="slack_webhook",
            message=slack_msg,
            username="airflow-alerts"
        ).execute(context)
        
        # Trigger PagerDuty
        from airflow.providers.pagerduty.operators.pagerduty import PagerdutyAlertOperator
        PagerdutyAlertOperator(
            integration_key=Variable.get("pagerduty_key"),
            summary=f"Airflow Pipeline Failure: {context['dag'].dag_id}",
            severity="critical"
        ).execute(context)

    @staticmethod
    def _alert_success(context):
        SlackWebhookOperator(
            task_id="slack_success",
            http_conn_id="slack_webhook",
            message=f":green_circle: Pipeline {context['dag'].dag_id} succeeded",
            username="airflow-alerts"
        ).execute(context)

# Initialize pipeline
pipeline = AzeercDataPipeline().dag
