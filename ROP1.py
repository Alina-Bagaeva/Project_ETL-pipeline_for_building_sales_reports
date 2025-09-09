import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.utils.dates import days_ago
from datetime import datetime

# Путь для временных файлов
TMP_DIR = "/tmp/airflow_clickhouse_upload"
os.makedirs(TMP_DIR, exist_ok=True)

# Параметры
TABLE_NAME = "ROP"  # новое имя таблицы в ClickHouse

# Создаём DAG
dag = DAG(
    dag_id='mariadb_to_clickhouse_ROP',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['example', 'bulk_upload'],
)

# SQL-запрос
SQL_QUERY = """
-- 1. Иеррхия папок номенклатур
WITH RECURSIVE folder_tree AS (
    SELECT 
        n.nomenclature_id,
        n.name AS folder_name,
        CAST(NULL AS INT) AS parent_id,
        1 AS level,
        JSON_ARRAY(n.name) AS full_path
    FROM nomenclatures n
    WHERE 
        n.root_folder_id = 0 
        AND n.deleted = 0 
        AND n.isFolder = 1
    UNION ALL
    SELECT 
        child.nomenclature_id,
        child.name,
        parent.nomenclature_id,
        parent.level + 1,
        JSON_ARRAY_APPEND(parent.full_path, '$', child.name)
    FROM nomenclatures child
    JOIN folder_tree parent 
        ON child.folder = parent.nomenclature_id
    WHERE 
        child.deleted = 0 
        AND child.isFolder = 1
        AND parent.level < 4 -- Ограничение глубины
),hierarcy_nomenclatures as (
	SELECT
	    n.nomenclature_code,
	    n.name,
	    CAST(JSON_VALUE(ft.full_path, '$[3]') as VARCHAR(50)) AS folder_3,
	    CAST(JSON_VALUE(ft.full_path, '$[2]') as VARCHAR(50)) AS folder_2,
	    CAST(JSON_VALUE(ft.full_path, '$[1]') as VARCHAR(50)) AS folder_1,
	    CAST(JSON_VALUE(ft.full_path, '$[0]') as VARCHAR(50)) AS root_folder
	FROM 
		nomenclatures n
	left join
		folder_tree ft 
		on n.folder=ft.nomenclature_id 
		and n.isFolder is null and n.deleted =0
	ORDER BY full_path
), 
-- 2. Иерархия папок отделов с различными уровнями вложенности
subdirectories as (
	select
	    d.department_id AS department_id,
	    d.department_name AS department_name,
	    d.parent_department_id AS parent_department_id,
	    d.department_id AS root_id
	from
	    departments d
	where
	    d.parent_department_id in (2831576, 2831586, 42561, 42560, 2124383, 2743029, 2743027)
	union all
	select
	    d.department_id AS department_id,
	    d.department_name AS department_name,
	    d.parent_department_id AS parent_department_id,
	    s.root_id AS root_id
	from
	    departments d
	join subdirectories s on
	    d.parent_department_id = s.department_id
), subdirectories1 as (
	select
	    d.department_id AS department_id,
	    d.department_name AS department_name,
	    d.parent_department_id AS parent_department_id,
	    d.department_id AS root_id
	from
	    departments d
	where
	    d.department_id in (2831542, 2831543, 59044, 2743041, 2831574, 2831602, 2831609, 2831612, 3081821)
	union all
	select
	    d.department_id AS department_id,
	    d.department_name AS department_name,
	    d.parent_department_id AS parent_department_id,
	    s.root_id AS root_id
	from
	    departments d
	join subdirectories1 s on
	    d.parent_department_id = s.department_id
), hierarchy_of_departments as (
	select
	    distinct s.department_id AS department_id,
	    s.department_name AS sector,
	    s.root_id AS original_parent_id,
	    d.department_name AS section,
	    d2.department_name AS department
	from
	    subdirectories s
	join departments d on
	    s.root_id = d.department_id
	join departments d2 on
	    d.parent_department_id = d2.department_id
	union all
	select
	    distinct s1.department_id AS department_id,
	    s1.department_name AS sector,
	    s1.root_id AS original_parent_id,
	    d.department_name AS section,
	    d.department_name AS department
	from
	    subdirectories1 s1
	join departments d on
	    s1.root_id = d.department_id
	union all
	select
	    d.department_id AS department_id,
	    d.department_name AS sector,
	    d.department_id AS original_parent_id,
	    d.department_name AS section,
	    d.department_name AS department
	from
	    departments d
	where
	    d.department_id in (2831576, 2831586, 2831572, 42561, 42560, 2124383, 2743029, 2743027)
), 
-- 3. Продажи. Первая часть -продажи со сменой автора реализации(отличается от автора счёта и реализация принаджелит партнёру)
sales as (
	SELECT 
		d.sbis_shipment_date,
		d.document_id,
		d.`number`,
		d2.department_id,
		d2.employee_id,
		"changed" as realization
	FROM 
		documents d 
	JOIN 
		documents d2 on d.`number` =d2.`number`
		and d.sbis_shipment_date>='2025-01-01'
		and d.sbis_shipment_date<'2025-08-01'
		and(d.shipment = 1)
		and(d.invoice_total_sum_calculated>0)
		and(d.deleted =0)
		and(d2.deleted =0)
		and(d2.document_type ="outbill")and(d.employee_id!=d2.employee_id)
	JOIN
		view_departments_with_root_folders d3 on d.department_id = d3.department_id
		and(d3.root_id = 59041)
	union all
	SELECT 
		d.sbis_shipment_date,
		d.document_id,
		d.`number`,
		d.department_id,
		d.employee_id,
		"not changed" as realization
	FROM 
		documents d
	where d.sbis_shipment_date>='2025-01-01'
		and d.sbis_shipment_date<'2025-08-01'
		and(d.shipment = 1) 
		and(d.invoice_total_sum_calculated>0)
		and(d.deleted =0)
)
-- 4. Итоговая витрина
SELECT
	DATE_FORMAT(s.sbis_shipment_date, '%Y-%m-%d') as date,
	s.`number` as number,
	CASE 
		when hd.department in ('Представители Киров', 'Представители Ижевск', 'Рефералы')
		then 'Представители'
		when hd.department is null
		then 'Не известен'
		else 'Офис'
	END as root_department,	
	IFNULL(hd.department , 'Не известен') as department,
	IFNULL(hd.section, 'Не известна') as section,
	IFNULL(hd.sector , 'Не известен') as sector,
	CASE 
		when e.last_name is null
		then 'Не известен'
		else concat(e.first_name, ' ', LEFT(e.last_name, 1),'.')
	END	 as employe_name,
	hnf.root_folder,
	hnf.folder_1,
	hnf.folder_2,
	hnf.folder_3,
	hnf.name,
	dtp.nomenclature_price_total,
	s.realization
FROM 
	sales s 
JOIN 
	documents_tabular_part dtp 
	on s.document_id =dtp.document_id
left JOIN 
	hierarcy_nomenclatures hnf
	on dtp.nomenclature_code = hnf.nomenclature_code
left JOIN 
	hierarchy_of_departments hd on s.department_id =hd.department_id
left join
	employees e on 
	s.employee_id=e.employee_id
"""

# --- Extract ---
def extract_from_file(**context):
    hook = MySqlHook(mysql_conn_id='sbis')
    df = hook.get_pandas_df(SQL_QUERY)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"vitrina_sales_{timestamp}.csv"
    file_path = os.path.join(TMP_DIR, file_name)
    df.to_csv(file_path, index=False)
    context['ti'].xcom_push(key='csv_path', value=file_path)

# --- Load ---
def load_to_file(**context):
    file_path = context['ti'].xcom_pull(key='csv_path')
    df = pd.read_csv(file_path)

    # Приведение типов
    df['date'] = pd.to_datetime(df['date']).dt.date
    df['number'] = df['number'].fillna('').astype(str)
    df['root_department'] = df['root_department'].fillna('').astype(str)
    df['department'] = df['department'].fillna('').astype(str)
    df['section'] = df['section'].fillna('').astype(str)
    df['sector'] = df['sector'].fillna('').astype(str)
    df['employe_name'] = df['employe_name'].fillna('').astype(str)
    df['root_folder'] = df['root_folder'].fillna('').astype(str)
    df['folder_1'] = df['folder_1'].fillna('').astype(str)
    df['folder_2'] = df['folder_2'].fillna('').astype(str)
    df['folder_3'] = df['folder_3'].fillna('').astype(str)
    df['name'] = df['name'].fillna('').astype(str)
    df['nomenclature_price_total'] = df['nomenclature_price_total'].astype(float)
    df['realization'] = df['realization'].fillna('').astype(str)

    data = list(zip(
        df['date'].tolist(),
        df['number'].tolist(),
        df['root_department'].tolist(),
        df['department'].tolist(),
        df['section'].tolist(),
        df['sector'].tolist(),
        df['employe_name'].tolist(),
        df['root_folder'].tolist(),
        df['folder_1'].tolist(),
        df['folder_2'].tolist(),
        df['folder_3'].tolist(),
        df['name'].tolist(),
        df['nomenclature_price_total'].tolist()
        df['realization'].tolist()
    ))

    ch_hook = ClickHouseHook(clickhouse_conn_id='click_house')
    client = ch_hook.get_conn()

    # Создание таблицы в ClickHouse, если нет
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        `date` Date,
        `number` String,
        `root_department` String,
        `department` String,
        `section` String,
        `sector` String,
        `employe_name` String,
        `root_folder` String,
        `folder_1` String,
        `folder_2` String,
        `folder_3` String,
        `name` String,
        `nomenclature_price_total` Float64,
        `realization` String
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(date)
    ORDER BY (date)
    """
    client.execute(create_table_sql)

    # Вставка данных
    client.execute(
        f"INSERT INTO {TABLE_NAME} (date, number, root_department, department, section, sector, employe_name, root_folder, folder_1, folder_2, folder_3, name, nomenclature_price_total, realization) VALUES",
        data
    )

# Определяем таски
extract_task = PythonOperator(
    task_id='extract_from_file',
    python_callable=extract_from_file,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_file',
    python_callable=load_to_file,
    provide_context=True,
    dag=dag,
)

extract_task >> load_task
