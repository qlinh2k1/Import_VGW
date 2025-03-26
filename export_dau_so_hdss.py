import redis
import pandas as pd
import os
import mysql.connector
import time
import threading
import logging
from datetime import datetime  # Import thư viện datetime

# MySQL connection details
db_config = {
    'host': '103.146.21.236',
    'user': 'mysql_voicegateway',
    'password': 'Kln8d24Bbd87BLkd94@',
    'database': 'jwdb_voicegateway'
}

# Redis connection details
redis_host = '103.150.240.66'
redis_port = 6379
redis_password = 'ph9UpCytm6fhZCdB'

# Configure logging
logging.basicConfig(filename='process_export_dauso.log',
                    level=logging.INFO,  # Adjust level as needed (DEBUG, INFO, WARNING, ERROR, CRITICAL)
                    format='%(asctime)s - %(levelname)s - %(message)s')

def get_processing_record():
    """Retrieve a record with c_tinhtrang = 'Processing' from the MySQL database."""
    #logging.info("Retrieving record with c_tinhtrang='Processing'.")
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor(dictionary=True)
    query = """
        SELECT id, export_id, c_group, c_status, c_tinhtrang, c_url 
        FROM outbox_excel_quan_ly_dau_so 
        WHERE c_tinhtrang = 'Processing' 
        LIMIT 1;
    """
    cursor.execute(query)
    record = cursor.fetchone()
    cursor.close()
    connection.close()
    return record

def update_status_and_url(record_id, outbox_id, file_path, file_name):
    """Update c_tinhtrang to 'Completed' and c_url with the file path for the specified record ID."""
    logging.info(f"Updating status to 'Completed' for record ID: {record_id}.")
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    update_query = """
        UPDATE app_fd_excel_quan_ly_dau_so 
        SET c_tinhtrang = %s, c_url = %s, dateCreated = NOW()
        WHERE id = %s;
    """
    c_tinhtrang_value = f'Completed - {file_name}'
    cursor.execute(update_query, (c_tinhtrang_value, file_path, record_id))
    update_query = """
           UPDATE outbox_excel_quan_ly_dau_so 
           SET c_tinhtrang = 'Completed' 
           WHERE id = %s;
       """
    cursor.execute(update_query, (outbox_id,))
    connection.commit()
    cursor.close()
    connection.close()

def process_group(r, group_name, c_status):
    """Process a Redis hash group, filtering based on the value (c_status) of the key (c_phone)."""
    logging.info(f"Processing group: {group_name} with c_status: {c_status}")
    filtered_data = []
    cursor = '0'

    while cursor != 0:
        cursor, keys_values = r.hscan(group_name, cursor=cursor, count=10)
        for c_phone, value in keys_values.items():
            if c_status == 'Available' and value == '':
                filtered_data.append({'c_group': group_name, 'c_phone': c_phone, 'c_status': 'Available'})
            elif c_status == 'Lock' and value == '1':
                filtered_data.append({'c_group': group_name, 'c_phone': c_phone, 'c_status': 'Lock'})
            elif c_status == '' and (value == '' or value == '1'):
                status = 'Available' if value == '' else 'Lock'
                filtered_data.append({'c_group': group_name, 'c_phone': c_phone, 'c_status': status})
        cursor = int(cursor)  # Convert cursor to integer

    return filtered_data

def process_multiple_groups(r, group_names, c_status):
    """Process multiple Redis hash groups, filtering based on the value (c_status) of the key (c_phone)."""
    logging.info(f"Processing multiple groups: {group_names}")
    all_filtered_data = []
    for group_name in group_names:
        filtered_data = process_group(r, group_name, c_status)
        all_filtered_data.extend(filtered_data)
    return all_filtered_data

def main_thread():
    time.sleep(10)

    # Lấy bản ghi có c_tinhtrang = 'Processing'
    record = get_processing_record()

    if record is None:
        return  # If no record is found, exit the function early

    # Kết nối đến Redis
    try:
        r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)

        if r.ping():
            logging.info("Connected to Redis successfully.")
        else:
            logging.error("Failed to connect to Redis.")

        outbox_id = record['id']
        record_id = record['export_id']
        group_name = record['c_group']
        c_status = record['c_status']  # Lấy giá trị c_status từ bản ghi

        start_time = time.time()

        if not group_name:  # Kiểm tra nếu group_name là None hoặc chuỗi rỗng
            group_names = ['HDSS_COL', 'HDSS_CSE', 'HDSS_COL_HNI', 'HDSS_SCL_HNI', 'HDSS_CUN']
            filtered_data = process_multiple_groups(r, group_names, c_status)
        else:
            logging.info(f"Processing single group: {group_name}")
            filtered_data = process_group(r, group_name, c_status)

        output_dir = f'/home/Export_dau_so/{record_id}/'
        os.makedirs(output_dir, exist_ok=True)

        # Lấy thời gian hiện tại và định dạng tên file
        current_time = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
        file_name = f'export_dauso_{current_time}.xlsx'
        output_file = os.path.join(output_dir, f'export_dauso_{current_time}.xlsx')
        relative_file_path = f'{record_id}/export_dauso_{current_time}.xlsx'

        if filtered_data:
            df = pd.DataFrame(filtered_data)
            df.to_excel(output_file, index=False)
            logging.info(f"Data has been written to {output_file}")
            update_status_and_url(record_id, outbox_id, relative_file_path, file_name)
            logging.info(f"Record ID {record_id} has been updated to 'Completed'.")
        else:
            logging.info("No data found with specified c_status values.")

        end_time = time.time()
        logging.info(f"Execution time: {end_time - start_time:.2f} seconds")

    except redis.exceptions.AuthenticationError:
        logging.error("Authentication failed. Please check your Redis username and password.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

def main():
    # Tạo và khởi động thread để xử lý công việc
    #logging.info("Starting main thread.")
    thread = threading.Thread(target=main_thread)
    thread.start()
    thread.join()

if __name__ == "__main__":
    main()
