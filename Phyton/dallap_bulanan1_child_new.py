from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import sum, col, when, lit
from minio import Minio
import os
import glob

def generate_query(year, month):
    return f"""
        select distinct {year} as tahun,
            {month} as bulan,
            "ProvinsiID" as id_provinsi,
            "KabupatenID" as id_kabupaten,
            "KecamatanID" as id_kecamatan,
            "KelurahanID" as id_kelurahan,
            (case when "KabupatenID" = 0 then null else "KabupatenID" end)::int as kab_lapor,
            (case when "KecamatanID" = 0 then null else "KecamatanID" end)::int as kec_lapor, 
            (case when "KelurahanID" = 0 then null else "KelurahanID" end)::int as kel_lapor
        from (select distinct b."ProvinsiID", b."KabupatenID", b."KecamatanID", b."KelurahanID"
        from sigabaru."dat_RegisterPPKBD" a
        inner join sigabaru."dat_PPKBD" b
        on a."PPKBDID" = b."PPKBDID"
        where a."Tahun"::int = coalesce({year}, to_char(now(), 'YYYY')::int) and a."Bulan"::int = coalesce(lpad({month}::varchar,2,'0')::int, to_char(now(), 'MM')::int)
        and b."Status" = 1
            union
        select distinct b."ProvinsiID", b."KabupatenID", b."KecamatanID", b."KelurahanID"
        from sigabaru."dat_KegiatanPPKS" a
        inner join sigabaru."dat_PPKS" b
        on a."PPKSID" = b."PPKSID"
        where a."Tahun"::int = coalesce({year}, to_char(now(), 'YYYY')::int) and a."Bulan"::int = coalesce(lpad({month}::varchar,2,'0')::int, to_char(now(), 'MM')::int)
        and b."Status" = 1
            union
        select distinct b."ProvinsiID", b."KabupatenID", b."KecamatanID", b."KelurahanID"
        from sigabaru."dat_KegiatanBKB" a
        inner join sigabaru."dat_BKB" b
        on a."BKBID" = b."BKBID"
        where a."Tahun"::int = coalesce({year}, to_char(now(), 'YYYY')::int) and a."Bulan"::int = coalesce(lpad({month}::varchar,2,'0')::int, to_char(now(), 'MM')::int)
        and b."Status" = 1
            union
        select distinct b."ProvinsiID", b."KabupatenID", b."KecamatanID", b."KelurahanID"
        from sigabaru."dat_KegiatanBKR" a
        inner join sigabaru."dat_BKR" b
        on a."BKRID" = b."BKRID"
        where a."Tahun"::int = coalesce({year}, to_char(now(), 'YYYY')::int) and a."Bulan"::int = coalesce(lpad({month}::varchar,2,'0')::int, to_char(now(), 'MM')::int)
        and b."Status" = 1
            union
        select distinct b."ProvinsiID", b."KabupatenID", b."KecamatanID", b."KelurahanID"
        from sigabaru."dat_KegiatanBKL" a
        inner join sigabaru."dat_BKL" b
        on a."BKLID" = b."BKLID"
        where a."Tahun"::int = coalesce({year}, to_char(now(), 'YYYY')::int) and a."Bulan"::int = coalesce(lpad({month}::varchar,2,'0')::int, to_char(now(), 'MM')::int)
        and b."Status" = 1
            union
        select distinct b."ProvinsiID", b."KabupatenID", b."KecamatanID", b."KelurahanID"
        from sigabaru."dat_KegiatanUPPKA" a
        inner join sigabaru."dat_UPPKA" b
        on a."UPPKAID" = b."UPPKAID"
        where a."Tahun"::int = coalesce({year}, to_char(now(), 'YYYY')::int) and a."Bulan"::int = coalesce(lpad({month}::varchar,2,'0')::int, to_char(now(), 'MM')::int)
        and b."Status" = 1
            union
        select distinct b."ProvinsiID", b."KabupatenID", b."KecamatanID", b."KelurahanID"
        from sigabaru."dat_KegiatanPIKRM" a
        inner join sigabaru."dat_PIKRM" b
        on a."PIKRMID" = b."PIKRMID"
        where a."TahunLapor"::int = coalesce({year}, to_char(now(), 'YYYY')::int) and a."BulanLapor"::int = coalesce(lpad({month}::varchar,2,'0')::int, to_char(now(), 'MM')::int)
        and b."Status" = 1
            union
        select distinct b."ProvinsiID", b."KabupatenID", b."KecamatanID", b."KelurahanID"
        from sigabaru."dat_KegiatanBalaiPenyuluhan" a
        inner join sigabaru."dat_BalaiPenyuluhan" b
        on a."BalaiPenyuluhanID" = b."BalaiPenyuluhanID"
        where a."TahunLapor"::int = coalesce({year}, to_char(now(), 'YYYY')::int) and a."BulanLapor"::int = coalesce(lpad({month}::varchar,2,'0')::int, to_char(now(), 'MM')::int)
        and b."Status" = 1
        ) src
    """

def run_spark_etl():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    jdbc_url_postgres = "jdbc:postgresql://10.0.0.0:5432/BKKBN"
    jdbc_properties_postgres = {
        "user": "postgres",
        "password": "awokawokawok",
        "driver": "org.postgresql.Driver"
    }

    current_year = datetime.now().year
    current_month = datetime.now().month
    previous_month = 12 if current_month == 1 else current_month - 1
    previous_2month = 11 if current_month == 1 else (12 if current_month == 2 else current_month - 2)
    current_month_str = "fa_laporan_dallap_bulanan_tabel01_new_"+str(datetime.now().month)
    previous_month_str = f"fa_laporan_dallap_bulanan_tabel01_new_{str(previous_month)}"
    previous_2month_str = f"fa_laporan_dallap_bulanan_tabel01_new_{str(previous_2month)}"
    previous_year = current_year if current_month > 1 else current_year - 1


    logger.info("Memulai SparkSession...")
    spark = SparkSession.builder \
        .appName("ETL") \
        .config("spark.jars.packages",
            "org.postgresql:postgresql:42.5.0") \
        .config("spark.driver.extraJavaOptions","-Djava.security.egd=file:/dev/./urandom") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.instances", "2") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .getOrCreate()

    # Generate query SQL untuk bulan sebelumnya dan saat ini
    query_2prev = generate_query(previous_year, previous_2month)
    query_prev = generate_query(previous_year, previous_month)
    query_curr = generate_query(current_year, current_month)

    df_2prev = spark.read.jdbc(url=jdbc_url_postgres, table=f"({query_2prev}) as subquery", properties=jdbc_properties_postgres)
    df_prev = spark.read.jdbc(url=jdbc_url_postgres, table=f"({query_prev}) as subquery", properties=jdbc_properties_postgres)
    df_curr = spark.read.jdbc(url=jdbc_url_postgres, table=f"({query_curr}) as subquery", properties=jdbc_properties_postgres)

    #generate to parquet
    output_path_2prev = "/database/airflow/parquet/"+previous_2month_str
    output_path_prev = "/database/airflow/parquet/"+previous_month_str
    output_path_curr = "/database/airflow/parquet/"+current_month_str

    df_2prev.write.mode("overwrite").parquet(output_path_2prev)
    df_prev.write.mode("overwrite").parquet(output_path_prev)
    df_curr.write.mode("overwrite").parquet(output_path_curr)

    logger.info("Export Parquet Selesai !")

    spark.stop()

    return output_path_2prev, output_path_prev, output_path_curr

def upload_to_minio(**kwargs):
    ti = kwargs['ti']
    output_path_2prev, output_path_prev, output_path_curr = ti.xcom_pull(task_ids='run_spark_etl')

    minio_client = Minio(
        "10.0.0.1:11984",
        access_key="admin",
        secret_key="awokawokawok",
        secure=False
    )

    bucket_name = "dwh-siga"
    parquet_files_2prev = glob.glob(output_path_2prev + "/*.parquet")
    parquet_files_prev = glob.glob(output_path_prev + "/*.parquet")
    parquet_files_curr = glob.glob(output_path_curr + "/*.parquet")
    current_month = datetime.now().month
    previous_month = 12 if current_month == 1 else current_month - 1
    previous_2month = 11 if current_month == 1 else (12 if current_month == 2 else current_month - 2)
    curr_file = "fa_laporan_dallap_bulanan_tabel01_new_"+str(datetime.now().month)
    prev_file = f"fa_laporan_dallap_bulanan_tabel01_new_{str(previous_month)}"
    prev2_file = f"fa_laporan_dallap_bulanan_tabel01_new_{str(previous_2month)}"
    
    # Hapus file jika sudah ada
    from minio.error import S3Error

    try:
        del_file_prev2 = minio_client.stat_object(bucket_name, f"dallap_bulanan/{prev2_file}.parquet")
    except S3Error as e:
        logging.warning(f"Previous month file not found in MinIO: {prev2_file}.parquet. Proceeding with upload.")

    try:
        del_file_prev = minio_client.stat_object(bucket_name, f"dallap_bulanan/{prev_file}.parquet")
    except S3Error as e:
        logging.warning(f"Previous month file not found in MinIO: {prev_file}.parquet. Proceeding with upload.")

    try: 
        del_file_curr = minio_client.stat_object(bucket_name, f"dallap_bulanan/{curr_file}.parquet")
    except S3Error as e:
        logging.warning(f"Previous month file not found in MinIO: {curr_file}.parquet. Proceeding with upload.")
    
    # Insert File ke MinIO
    if parquet_files_2prev:
        minio_client.fput_object(bucket_name, f"dallap_bulanan/{prev2_file}.parquet", parquet_files_2prev[0])
    else:
        raise FileNotFoundError("No Parquet file for Previous Month found")
    
    if parquet_files_prev:
        minio_client.fput_object(bucket_name, f"dallap_bulanan/{prev_file}.parquet", parquet_files_prev[0])
    else:
        raise FileNotFoundError("No Parquet file for Previous Month found")

    if parquet_files_curr:
        minio_client.fput_object(bucket_name, f"dallap_bulanan/{curr_file}.parquet", parquet_files_curr[0])
    else:
        raise FileNotFoundError("No Parquet file for Current Month found")

# Definisi DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'dallap_bulanan_tabel01_child_new',
    default_args=default_args,
    description='DAG untuk generate parquet dan upload ke MinIO',
    schedule_interval='01 2 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    etl_task = PythonOperator(
        task_id='run_spark_etl',
        python_callable=run_spark_etl,
        execution_timeout=timedelta(minutes=30),
    )

    upload_task = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_to_minio,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )

    etl_task >> upload_task
