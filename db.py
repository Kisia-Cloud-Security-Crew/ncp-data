from datetime import datetime

import pymysql


# MySQL 연결 설정 함수
def connect_to_db(host, user, password, database):
    db = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    return db

# 로그 삽입 함수
def insert_logs(cursor, logs, log_type_table):
    for log in logs:
        try:
            log_detail = log.get('logDetail', 'No detail')
            log_time = log.get('logTime')
            log_type = log.get('logType', 'Unknown')
            server_name = log.get('servername', 'Unknown')

            if "{name=" in server_name:
                server_name = server_name.split("{name=")[-1].split("}")[0]

            if log_time:
                log_time = datetime.fromtimestamp(int(log_time) / 1000)

            sql = f"INSERT INTO {log_type_table} (log_time, log_type, servername, log_detail) VALUES (%s, %s, %s, %s)"
            cursor.execute(sql, (log_time, log_type, server_name, log_detail))
        except Exception as e:
            print(f"Error inserting log: {e}")

    cursor.connection.commit()