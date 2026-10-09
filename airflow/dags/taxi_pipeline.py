from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator


with DAG(

    dag_id="taxi_streaming_pipeline",

    start_date=datetime(
        2026,
        1,
        1
    ),

    schedule=None,

    catchup=False,

    tags=[
        "taxi",
        "kafka",
        "spark",
        "cassandra"
    ]

) as dag:


    check_kafka = BashOperator(

        task_id="check_kafka",

        bash_command="""
        python -c "import socket; s=socket.create_connection(('kafka', 29092), 10); s.close(); print('Kafka is reachable')"
        """

    )


    check_cassandra = BashOperator(

        task_id="check_cassandra",

        bash_command="""
        python -c "import socket; s=socket.create_connection(('cassandra', 9042), 10); s.close(); print('Cassandra is reachable')"
        """

    )


    validate_pipeline = BashOperator(

        task_id="validate_pipeline",

        bash_command="""
        python -c "import socket; s=socket.create_connection(('spark-master', 7077), 10); s.close(); print('Spark Master is reachable')"
        python -c "import socket; s=socket.create_connection(('spark-worker', 8081), 10); s.close(); print('Spark Worker is reachable')"
        echo "Taxi streaming infrastructure validated."
        """

    )


    check_kafka >> check_cassandra >> validate_pipeline