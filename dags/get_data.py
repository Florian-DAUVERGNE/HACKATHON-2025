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
    python_callable=functions.make_final_df,
    op_kwargs={},
    dag=dag,
)

make_note = PythonOperator(
    task_id='make_note',
    python_callable=functions.make_note,
    op_kwargs={},
    dag=dag,
)

send_mail = get_dataset_energy_production = PythonOperator(
    task_id='send_mail',
    python_callable=functions.send_mail,
    op_kwargs={},
    dag=dag,
)

get_dataset >> make_note >> send_mail