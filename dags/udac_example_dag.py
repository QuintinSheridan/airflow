from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
AWS_KEY = ''
AWS_SECRET = ''


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 9, 20),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

# udac_example_dag

# dag = DAG('udac_example_dag',
#           default_args=default_args,
#           description='Load and transform data in Redshift with Airflow',
#           schedule_interval='0 * * * *'
#         )

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    table = 'staging_events',
    create_table = '''
    CREATE TABLE IF NOT EXISTS public.staging_events (
        artist varchar(256),
        auth varchar(256),
        firstname varchar(256),
        gender varchar(256),
        iteminsession int4,
        lastname varchar(256),
        length numeric(18,0),
        "level" varchar(256),
        location varchar(256),
        "method" varchar(256),
        page varchar(256),
        registration numeric(18,0),
        sessionid int4,
        song varchar(256),
        status int4,
        ts int8,
        useragent varchar(256),
        userid int4
    );''',
    s3 = "s3://udacity-dend/log_data/2018/11",
    json_path = "'s3://udacity-dend/log_json_path.json'",
    aws_key = AWS_KEY,
    aws_secret = AWS_SECRET,
    dag = dag
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    table = 'staging_songs',
    create_table = '''
        CREATE TABLE IF NOT EXISTS public.staging_songs (
            num_songs int4,
            artist_id varchar(256),
            artist_name varchar(256),
            artist_latitude numeric(18,0),
            artist_longitude numeric(18,0),
            artist_location varchar(256),
            song_id varchar(256),
            title varchar(256),
            duration numeric(18,0),
            "year" int4
        );''',
    s3 = 's3://udacity-dend/song_data/A/A/A',
    json_path = "'auto'",
    dag = dag,
    aws_key = AWS_KEY,
    aws_secret = AWS_SECRET
 )


load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    create_table = '''
    CREATE TABLE IF NOT EXISTS public.songplays (
        playid varchar(32) NOT NULL,
        start_time timestamp NOT NULL,
        userid int4 NOT NULL,
        "level" varchar(256),
        songid varchar(256),
        artistid varchar(256),
        sessionid int4,
        location varchar(256),
        user_agent varchar(256),
        CONSTRAINT songplays_pkey PRIMARY KEY (playid)
        );''',
    insert_table = 'songplays',
    insert_select = SqlQueries.songplay_table_insert 
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    create_table = '''
    CREATE TABLE IF NOT EXISTS public.users (
        userid int4 NOT NULL,
        first_name varchar(256),
        last_name varchar(256),
        gender varchar(256),
        "level" varchar(256),
        CONSTRAINT users_pkey PRIMARY KEY (userid)
    );
    ''',
    insert_table = 'users',
    insert_select = SqlQueries.user_table_insert 
)


load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    create_table = '''
    CREATE TABLE IF NOT EXISTS public.songs (
        songid varchar(256) NOT NULL,
        title varchar(256),
        artistid varchar(256),
        "year" int4,
        duration numeric(18,0),
        CONSTRAINT songs_pkey PRIMARY KEY (songid)
    );
    ''',
    insert_table = 'songs',
    insert_select = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    create_table = '''
    CREATE TABLE IF NOT EXISTS public.artists (
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)
    );
    ''',
    insert_table = 'artists',
    insert_select = SqlQueries.artist_table_insert 
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    create_table = '''
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp NOT NULL,
        hour int4,
        day int4,
        week int4,
        month varchar(256),
        year int4,
        weekday varchar(256),
        CONSTRAINT time_pkey PRIMARY KEY (start_time)
    );
    ''',
    insert_table = 'time',
    insert_select = SqlQueries.time_table_insert 
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    test_for_rows = ['SELECT count(*) FROM staging_events',
                   'SELECT count(*) FROM staging_songs',
                   'SELECT count(*) FROM users',
                   'SELECT count(*) FROM artists',
                   'SELECT count(*) FROM songs',
                   'SELECT count(*) FROM time'],
    test_sql = [],
    test_results = [],
    retries = 1
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
