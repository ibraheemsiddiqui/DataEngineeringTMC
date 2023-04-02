from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
# from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'linux_work',
    # 'depends_on_past': False,
    'start_date': days_ago(0),
    'email': ['ibraheem.siddique@tmcltd.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id='Airflow_Web_Scraping',
    default_args=default_args,
    description='Scraping text data of Lush Products',
    schedule_interval=timedelta(days=1)
)

def ScrapingCode():
    from bs4 import BeautifulSoup
    import requests
    import pandas as pd


    temp = []
    items_list = []
    price_list= []
    topics_url = 'https://www.lushusa.com/face/cleansers-scrubs/?cgid=cleansers-scrubs&start=0&sz=28%27'
    response = requests.get(topics_url).text
    doc=BeautifulSoup(response,'html.parser')
    topic_title_tags=doc.find_all('span',class_='visually-hidden')
    price_title_tags=doc.find_all('span',class_='tile-price align-middle font-weight-bold')
    for item in topic_title_tags:
        temp.append(item.text)   
    for i in range(1,len(temp)-1):
        x=temp[i].strip("Add Wishlist").strip("to")
        items_list.append(x)
    for item in price_title_tags:
        price_list.append(item.text)
        if(len(price_list)==20):
            break

    data = pd.DataFrame({'Products':items_list,'Price':price_list})
    data.to_excel("Output.xlsx",sheet_name='sheet1',index=False)
    print("DONE")


task = BashOperator(
    task_id = 'start_task',
    bash_command = 'echo start',
    dag = dag
)


task1 = PythonOperator(
    task_id = 'ScrapingCode' ,
    python_callable = ScrapingCode ,
    dag = dag
)

task2 = BashOperator(
    task_id = 'finish_task',
    bash_command = 'echo finish',
    dag = dag
)

task >> task1 >> task2


