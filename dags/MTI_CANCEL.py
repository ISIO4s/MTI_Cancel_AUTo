from airflow import DAG
from airflow.decorators import task, dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.timetables.trigger import CronTriggerTimetable
from datetime import datetime
import tomllib
import pendulum
import os
from os import path
import oracledb
import logging
import pandas as pd
import requests
import pendulum

with open("./config/mticanecel.toml", "rb") as f:
    cfg = tomllib.load(f)

local = cfg["variable"]["UTC"]
local_tz = pendulum.timezone (local)
currentDateAndTime = pendulum.now(tz=local_tz)
currentDate = currentDateAndTime.strftime("%Y-%m-%d")
currentTime = currentDateAndTime.strftime("%H:%M:%S")


url = cfg["variable"]["prbgateway_url"]

# Help Function
def ConOracle():
    try:
        env = os.getenv('ENV', 'xininsure_preprod')
        try:
            db = cfg["oracle"][env]
        except KeyError as e:
            raise KeyError(f"ไม่พบ config oracle สำหรับ environment '{env}' ใน TOML: {e}")

        db_host = db["host_xininsure"]
        db_port = int(db["port_xininsure"])
        db_username = db["username_xininsure"]
        db_password = db["password_xininsure"]
        db_name = db["dbname_xininsure"]
        
        dsn_name = oracledb.makedsn(db_host, db_port, service_name=db_name)
        conn = oracledb.connect(user=db_username, password=db_password, dsn=dsn_name)

        cursor = conn.cursor()
        print(f"Connecting database {db_name}")
        return cursor, conn
    except oracledb.Error as error:
        message = f"เกิดข้อผิดพลาดในการเชื่อมต่อกับ Oracle DB : {error}"
        # ##send_flex_notification_start(message)
        print("เกิดข้อผิดพลาดในการเชื่อมต่อกับ Oracle DB:", error)
        return message, None

def Get_Holidays():
    cursor, conn = ConOracle()
    try:
        cursor.execute(
            """
                SELECT * FROM XININSURE.HOLIDAY h
                WHERE h.FISCALYEAR = extract(year from sysdate)
            """
        )
        df = pd.DataFrame(
            cursor.fetchall(), columns=[desc[0] for desc in cursor.description]
        )
        #print(df)
        formatted_table = df.to_markdown(index=False)
        #print(f"\n{formatted_table}")
        print(f"Get data successfully")
        return df
    except oracledb.Error as e:
        print(f"Get_holidays : {e}")
        return None
    

def Check_Holiday(df):
    try:
        holiday_dates = df["HOLIDAYDATE"].dt.strftime("%Y-%m-%d")
        # print(f"Holiday Dates: \n  {holiday_dates}")
        formatted_table = holiday_dates.to_markdown(index=False)
        #print(f"\n{formatted_table}")
        print(f"Today : {currentDate}")
        if currentDate in holiday_dates.values:
            print("Today is a holiday. Ending DAG.")
            return "Holiday_path"
        else:
            print("Today is not a holiday. Proceeding with work path.")
            return "Work_path"
    except Exception as e:
        print(f"Check Holiday error: {e}")
        raise e
    
def discord_notify(mes):

    sodium = cfg["variable"]["S0duim"]
    payload = {"content":mes}
    response = requests.post(sodium, json=payload)

def get_Actionid(conn,cursor,actioncode):
    """
    กันกรณี Actionid บน pro กับ pre ไม่ตรงกัน
    """
    try:
        param_data ={"ACTIONCODE": actioncode,}
        with open("/opt/airflow/query/get_actioncode.sql","r",encoding="utf-8") as file:
            sql = file.read()
        cursor.execute(sql,param_data)
        row = cursor.fetchone()
        return row[0] if row else None
    except oracledb.Error as e:
        message = f'เกิดข้อผิดพลาด: {e}'
        print(f"เจ๊ง: {message}")
        discord_notify(message)

def insert_saleaction(df,actioncode):
    
    try:
        cursor,conn = ConOracle()
        with open("/opt/airflow/query/insert_action.sql","r",encoding="utf-8") as file:
            sql = file.read()
        actionid = get_Actionid(conn,cursor,actioncode)
        
        if actionid is None:
            raise ValueError(f"ไม่พบ ACTIONID สำหรับ ACTIONCODE '{actioncode}'")
        print(f"actionid{actionid}")
        for _,row in df.iterrows():
            saleid = row["SALEID"]
            param = {
            "SALEID":saleid,
            "ACTIONID":actionid,
            "ACTIONSTATUS":"Y",
            "REQUESTREMARK":None,
            "ACTIONREMARK":None
            }

            if actioncode == 'VO04':
                param["ACTIONREMARK"] = "AUTO_API_CANCEL"
                cursor.execute(sql,param)
            elif actioncode == 'CMT72':
                param["ACTIONREMARK"] = "AUTO_API_CANCEL ไม่เรียกเก็บค่ำคุ้มครอง พ.ร.บ"
                cursor.execute(sql,param)
            elif actioncode == 'CMT105.1':
                param["ACTIONSTATUS"]="W"
                param["REQUESTREMARK"]= f"AUTO_API_CANCEL {row["MESSAGES"]}"
                cursor.execute(sql,param)
            print(f"Insert data saleid: {saleid} actioncode: {actioncode}")
        conn.commit()
        
    except oracledb.Error as e:
        message = f'เกิดข้อผิดพลาด: {e}'
        print(f"เจ๊ง: {message}")
        conn.rollback()
        discord_notify(message)

def prbgateway(df):
    username = cfg["variable"]['username']
    password = cfg["variable"]['password']

    try:
        fd = {
            'username':username,
            'password':password
        }
        url_auth = f"{url}/auth/token"
        url_cancel=f"{url}/MTIPRB/CancelBySaleId"
        respon = requests.post(url_auth,data=fd)

        if respon.status_code == 200:
            token = respon.json()
            print(token)
        else:
            logging.error("cant login")
        for index,row in df.iterrows():
            body = {
                    "saleid":str(row["SALEID"]),
                    }
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                }       

            response = requests.post(url_cancel,headers=headers,json=body)
            response_data = response.json()
            logging.error("===========respon_api===========")
            print(response_data)
            if response_data:
                api_status = response_data["status"]
            else:
                api_status = False
            
            if api_status is False:
                message_text = response_data["messages"][0].get("messageText", None)
                print(message_text)
                df.loc[index, 'APISTATUS'] = 'N'
                df.loc[index, 'MESSAGES'] = message_text
            else:
                df.loc[index, 'APISTATUS'] = 'Y'
        return df
    except Exception as e:
        mes = f"เกิดข้อผิดพลาด = {e}"
        logging.error(mes)
        discord_notify(mes)
# ==================== DAG Configuration ====================
@dag(
        dag_id="MTI-CANCEL-AIRFLOW",
        start_date=pendulum.datetime(2025, 10, 1, tz=local_tz),
        schedule=CronTriggerTimetable("*/5 8-20 * * *", timezone="Asia/Bangkok"),
        catchup=False,
        tags=["MTI"],
)
def main():

    @task.branch
    def check_holiday(**kwargs):
        """
          ตรวจสอบว่าวันนี้เป็นวันหยุดหรือไม่
        """
        ti = kwargs["ti"]
        task_id = kwargs['task_instance'].task_id
        try_number = kwargs['task_instance'].try_number
        message = f"Processing task {task_id}, try_number {try_number}"
        print(f"{message}")
        
        try: 
            df = Get_Holidays()
            result = Check_Holiday(df)
            #print(result)
            message = f"Continue with {result}"
            if result == "Holiday_path":
                return "Holiday_path"
            else:          
                return "Work_path"
        except Exception as e:
            message = f"Fail with task {task_id} \n error : {e}"
            print(f"check_holiday : {e}")
            discord_notify(message)
            print(f"{message}")
    
    @task # ดึงงานยกเลิก
    def get_datacancel(**kwargs):
        ti = kwargs["ti"]
        task_id = kwargs['task_instance'].task_id
        try_number = kwargs['task_instance'].try_number
        message = f"Processing task {task_id}, try_number {try_number}"

        try:
            print("==========start==========")
            mock = ""
            mockdate = currentDateAndTime
            qdate = datetime.strptime(mock, "%Y-%m-%d") if mock else mockdate
            print(f"วันที่:{qdate}")
            cursor,conn = ConOracle()
            params = {"qdate": qdate}
            #params = {"qdate": qdate}
            logging.info("==========Get data==========")
            path_file = '/opt/airflow/query/get_data.sql'
            with open(path_file,"r",encoding="utf-8") as file:
                get_data_sql = file.read()

            cursor.execute(get_data_sql,params)
            df = pd.DataFrame(
                    cursor.fetchall(), columns=[desc[0] for desc in cursor.description]
                )
            print("======== start df original ============")
            print("จำนวนงานทั้งหมด:", len(df))
            print(df.head().to_markdown(index=False))
            print("======== end df original ============")
            return { 'df_cancel_work': df}

        except oracledb.Error as e:
            logging.error(f"Get_Data : {e}")
            message = f'เกิดข้อผิดพลาด: {e}'
            discord_notify(message)
            
    @task # insert รับทราบงาน
    def got_it(**kwargs):
        ti = kwargs["ti"]
        task_id = kwargs['task_instance'].task_id
        try_number = kwargs['task_instance'].try_number
        message = f"Processing task {task_id}, try_number {try_number}"
        
        result = ti.xcom_pull(task_ids="main_process_group.get_datacancel", key="return_value")
        df = result.get("df_cancel_work",pd.DataFrame())
        try:
            if not result:
                logging.info("ไม่มีงานยกเลิก ณ ขนาดนี้")
                return 0
            if df.empty:
                print("DataFrame is empty. Exiting task.")
                return 0
            else:
                logging.info("============Insert============")
                insert_saleaction(df,"VO04")
                new_df = prbgateway(df)

            return { 'df_apicancel': new_df}
        except oracledb.Error as e:
            print(f"Get_Data : {e}")
            message = f'เกิดข้อผิดพลาด : {e}'
            discord_notify(message)
        except Exception as e:
            print(f"Get_Data : {e}")
            message = f'เกิดข้อผิดพลาด : {e}'
            discord_notify(message)

    @task # insert ผลตอบรับผลังจาก API
    def insert_saleaction_after(**kwargs):
        ti = kwargs["ti"]
        task_id = kwargs['task_instance'].task_id
        try_number = kwargs['task_instance'].try_number
        message = f"Processing task {task_id}, try_number {try_number}"

        result = ti.xcom_pull(task_ids="main_process_group.got_it", key="return_value")
        df = result.get("df_apicancel",pd.DataFrame())
        try:
            
            
            df_pass = df.query("APISTATUS == 'Y'") if df is not None and not df.empty else pd.DataFrame()
            df_fail = df.query("APISTATUS == 'N'") if df is not None and not df.empty else pd.DataFrame()
            df_pass_table = df_pass.to_markdown(index=False)
            df_fail_table = df_fail.to_markdown(index=False)


            logging.critical("====================งานที่ผ่าน=====================")
            print(df_pass_table)
            insert_saleaction(df_pass,"CMT72")
            logging.critical("====================งานที่ไม่ผ่าน=====================")
            print(df_fail_table)
            insert_saleaction(df_fail,"CMT105.1")

        except Exception as e:
            logging.info(f"Get_Data : {e}")
            message = f'เกิดข้อผิดพลาด : {e}'
            discord_notify(message)
    
    
    #EmptyTask
    start = EmptyOperator(task_id="start_dag", trigger_rule="none_failed_min_one_success")
    end = EmptyOperator(task_id="end_dag", trigger_rule="none_failed_min_one_success")
    holiday_path = EmptyOperator(task_id="Holiday_path", trigger_rule="none_failed_min_one_success")
    work_path = EmptyOperator(task_id="Work_path", trigger_rule="none_failed_min_one_success")

    #ProcessTask
    check_holiday_task = check_holiday()
    
    # automation flow 
    @task_group(group_id = "main_process_group")
    def main_process_group():
        get_datacancel_task = get_datacancel()
        got_it_task = got_it()
        insert_saleaction_after_task = insert_saleaction_after()

        get_datacancel_task >> got_it_task >>insert_saleaction_after_task
    

    (
        start>>check_holiday_task >> [holiday_path, work_path],
        holiday_path >> end,
        work_path >> main_process_group() >> end,
    )

main()