import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
import functions


dag = DAG(
 dag_id="HACKATHON",
 start_date=airflow.utils.dates.days_ago(0),
 schedule_interval="0 0 */3 * *"
)

get_dataset = PythonOperator(
    task_id='get_dataset',
    python_callable=functions.make_disruptions_pag_DF,
    op_kwargs={"dataset_path": "Solar_Energy_Production.csv","values": ['name', 'id', 'address', 'date', 'kWh']},
    dag=dag,
)

send_mail = get_dataset_energy_production = PythonOperator(
    task_id='send_mail',
    python_callable=functions.send_mail,
    op_kwargs={},
    dag=dag,
)