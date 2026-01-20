import pytest
from airflow.models import DagBag
from dags.dag_testing_assignment import transform_data

# Fixtures
@pytest.fixture
def sample_data():
    return [
        {"name": "apple", "price": 10},
        {"name": "banana", "price": 5},
    ]


# Unit Tests
def test_transform_data(sample_data):
    result = transform_data(sample_data)

    assert result == [
        {"name": "APPLE", "price": 10, "price_with_tax": 11.0},
        {"name": "BANANA", "price": 5, "price_with_tax": 5.5},
    ]


def test_transform_data_empty():
    result = transform_data([])
    assert result == []


# DAG Integrity Test
def test_dag_integrity():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)

    # DAG should load without errors
    assert not dagbag.import_errors

    dag = dagbag.get_dag("data_validation_dag")
    assert dag is not None

    # Validate structure
    assert len(dag.tasks) >= 3

    task_ids = [task.task_id for task in dag.tasks]
    assert "extract_task" in task_ids
    assert "transform_task" in task_ids
    assert "load_task" in task_ids
