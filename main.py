import os

from api import make_api_request
from db import connect_to_db, insert_logs
from dotenv import load_dotenv

uri = f'/api/kr-v1/logs/search'
api_url = f'https://cloudloganalytics.apigw.ntruss.com{uri}'

# .env 파일 로드
load_dotenv()

# 환경 변수에서 민감한 정보 읽기
api_key = os.getenv('API_KEY')
access_key = os.getenv('ACCESS_KEY')
secret_key = os.getenv('SECRET_KEY')
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

# 로그 수집 및 저장 함수
def collect_and_store_logs(log_type, table_name, page_size=100, max_pages=10):
    db = connect_to_db(db_host, db_user, db_password, db_name)
    cursor = db.cursor()

    page_no = 1
    logs_collected = 0
    
    while page_no <= max_pages:
        body = {
            "logTypes": log_type,
            "pageSize": page_size,
            "pageNo": page_no
        }

        response = make_api_request(api_url, "/api/kr-v1/logs/search", access_key, secret_key, api_key, body)

        if response.status_code == 200:
            logs = response.json().get('result', {}).get('searchResult', [])
            if logs:
                insert_logs(cursor, logs, table_name)
                logs_collected += len(logs)
                print(f"Page {page_no}: {len(logs)} logs collected.")
            else:
                print(f"No more logs found on page {page_no}.")
                break
        else:
            print(f"Error: {response.status_code}, Message: {response.text}")
            break

        page_no += 1

    print(f"Total logs collected: {logs_collected}")
    db.close()

# 메인 함수 실행
if __name__ == "__main__":
    collect_and_store_logs("SYSLOG", "syslog_table", page_size=100, max_pages=10)
    collect_and_store_logs("audit_log", "audit_log_table", page_size=100, max_pages=10)
    collect_and_store_logs("cdb_mysql_error, cdb_mysql_audit, cdb_mysql_slow", "mysql_log_table", page_size=100, max_pages=10)