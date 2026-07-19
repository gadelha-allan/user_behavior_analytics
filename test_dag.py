import pytest
from airflow.models import DagBag

def test_dag_loaded():

    dagbag = DagBag(dag_folder='.', include_examples=False)
    
    assert len(dagbag.import_errors) == 0, f"Erros de importação: {dagbag.import_errors}"
    
    dag_id = 'user_analytics_dag'
    assert dag_id in dagbag.dags, f"A DAG {dag_id} não foi encontrada!"
    
    dag = dagbag.dags[dag_id]
    assert dag.catchup is False, "A DAG não deve ter o catchup ativado"
    assert dag.max_active_runs == 1, "A DAG deve limitar a concorrência a 1"
