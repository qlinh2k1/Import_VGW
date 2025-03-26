import os
import pandas as pd
import redis
import mysql.connector
import json
import uuid
from datetime import datetime, timezone, timedelta
import logging
import threading
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("import_dauso_log.log"),
        logging.StreamHandler()
    ]
)

def process_number_to_redis(request, connection_string):
    try:
        if request and request.get("redis_host") and request.get("ds_key_group"):
            logging.info(f"Start Add_Delete_number_to_redis request: {json.dumps(request)}")

            ds_key_group = request["ds_key_group"].split(',')
            cache = redis.StrictRedis(host=request["redis_host"], port=request["redis_port"],
                                      password=request["redis_pass"], decode_responses=True)
            _json = ""

            if request["src_status"] == "add":
                ds_add = request["src_number"]
                for number in ds_add:
                    if number:
                        number_str = str(number).strip()  # Chuyển đổi số điện thoại thành chuỗi
                        cache.hset(request["ds_key_group"], number_str, "")
                        cache.set(f"CHECK-LOCK-CALLER-{number_str}", "")
                        logging.info(f"Add_One_number_to_redis {number_str} to {request['ds_key_group']}")
                _json = json.dumps([str(num) for num in ds_add])  # Đảm bảo dữ liệu JSON là chuỗi

            elif request["src_status"] == "delete":
                ds_add = request["src_number"]
                for number in ds_add:
                    if number:
                        number_str = str(number).strip()  # Chuyển đổi số điện thoại thành chuỗi
                        cache.hdel(request["ds_key_group"], number_str)
                        logging.info(f"Delete_One_number_to_redis {number_str} from {request['ds_key_group']}")
                _json = json.dumps([str(num) for num in ds_add])  # Đảm bảo dữ liệu JSON là chuỗi

            if _json and request.get("table_name"):
                execute_sql_insert(_json, request["table_name"], request["src_status"], request["ds_key_group"],
                                   connection_string)

    except Exception as ex:
        logging.error(f"Error Get_list_from_redis {str(ex)}")

def execute_sql_insert(json_data, tbl_name, src_status, src_group, connection_string):
    connection = mysql.connector.connect(**connection_string)
    cursor = connection.cursor()

    data_list = json.loads(json_data)

    # Đặt múi giờ UTC+7
    timezone_offset = timedelta(hours=7)
    timezone_utc_plus_7 = timezone(timezone_offset)
    current_time = datetime.now(timezone_utc_plus_7).strftime('%Y/%m/%d %H:%M:%S')

    sql_statement = f'''
        INSERT INTO {tbl_name} (id, dateCreated, dateModified, createdBy, createdByName, modifiedBy, modifiedByName, c_Status, c_TimeStart, c_Source, c_Description)
        VALUES (%s, %s, %s, 'system', 'system', 'system', 'system', %s, %s, %s, %s)
    '''

    for value in data_list:
        id = str(uuid.uuid4())
        data = (id, current_time, current_time, src_status.upper(), current_time, value, src_group)
        cursor.execute(sql_statement, data)

    connection.commit()
    cursor.close()
    connection.close()

def main():
    connection_string = {
        'host': '103.146.21.236',
        'user': 'mysql_voicegateway',
        'password': 'Kln8d24Bbd87BLkd94@',
        'database': 'jwdb_voicegateway'
    }

    connection = mysql.connector.connect(**connection_string)
    cursor = connection.cursor(dictionary=True)

    cursor.execute("SELECT * FROM outbox_form_import_dauso WHERE status = 'Pending' limit 1")
    pending_records = cursor.fetchall()

    for record in pending_records:
        # Tạo đường dẫn file dựa trên import_id và c_FileUpload
        file_path = f"/var/lib/docker/volumes/7f12dfc7a887625c1a15d8e653a1e27cb76259fa1c5aa9f2b980e240d3a1d7d2/_data/app_formuploads/form_import_dauso/{record['import_id']}/{record['c_FileUpload']}"
        #file_path = f"D:/tls_import_dau_so.xlsx"  # Đường dẫn tới file Excel

        # Đọc số điện thoại từ file Excel và xử lý giữ số 0 ở đầu
        excel_data = pd.read_excel(file_path, sheet_name='Sheet1', dtype={'PHONE_NUMBER': str})
        phone_numbers = excel_data['PHONE_NUMBER'].dropna().tolist()

        request = {
            "redis_host": record["c_redis_host"],
            "redis_port": int(record["c_redis_port"]),
            "redis_pass": record["c_redis_pass"],
            "ds_key_group": record["c_nhom"],
            "src_number": [str(num).strip() for num in phone_numbers],  # Sử dụng danh sách số điện thoại từ Excel và chuyển sang chuỗi
            "src_status": record["c_type"],
            "table_name": record["c_table_name"]
        }

        process_number_to_redis(request, connection_string)

        update_sql = f"UPDATE outbox_form_import_dauso SET status = 'Completed' WHERE id = {record['id']}"
        cursor.execute(update_sql)
        connection.commit()
        logging.info(f"Process import dau so successfully! with outbox_id: {record['id']}")

    cursor.close()
    connection.close()

def run_main_with_delay():
    while True:
        main()
        time.sleep(10)

if __name__ == "__main__":
    thread = threading.Thread(target=run_main_with_delay)
    thread.start()