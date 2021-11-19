import configparser
from typing import Dict
import psycopg2
import requests
import json
import csv
from psycopg2 import  Error
config = configparser.ConfigParser()
config.read('configs.ini')
endpoint = config['endpoints']['wagestream_endpoint']
token = json.load(open("token.json"))
headers = {f"Authorization": token}

def retrieve_token():
    URI = endpoint + 'auth/login'
    r = requests.post(url=URI, data=token).json()
    return r

def request_json(parameter):
    URI = endpoint+parameter
    token = 'Bearer ' + retrieve_token()['access_token']
    r = requests.get(url = URI, headers = {"Authorization" : token})
    return r.json()

def malformed_employees():
    data = request_json('employees')
    print("Invalid normal codes:")
    for employee in data['data']:
        if'wsr' in employee['full_name'] and employee['current_state'] != 'ENROLLING' and len(employee['properties']['payment_sort_code']) !=6:
            print(employee['full_name'], employee['properties']['payment_sort_code'])

    def offline_valid_international():
        print("Invalid international codes:")
        for employee in data['data']:
            if'wsr' in employee['full_name'] and employee['current_state'] != 'ENROLLING' and 12 > len(employee['properties']['payment_sort_code']) < 8 :
                print(employee['full_name'], employee['properties']['payment_sort_code'])
    offline_valid_international()

def offline_malformed_employees():
    data = json.load(open("EmployeeRequest/data.json"))

    print("Invalid normal codes:")
    for employee in data['data']:
        if'wsr' in employee['full_name'] and employee['current_state'] != 'ENROLLING' and len(employee['properties']['payment_sort_code']) !=6:
            print(employee['full_name'], employee['properties']['payment_sort_code'])

    def offline_valid_international():
        print("Invalid international codes:")
        for employee in data['data']:
            if'wsr' in employee['full_name'] and employee['current_state'] != 'ENROLLING' and 12 > len(employee['properties']['payment_sort_code']) < 8 :
                print(employee['full_name'], employee['properties']['payment_sort_code'])
    offline_valid_international()

#Wrotes employeee data to csv
def process_employee_transfers():
    max_transfers = get_transfer_data()
    employees = request_json('employees')['data']
    employeeFile = csv.writer(open("Employees.csv","w"))
    employeeFile.writerow(["name","available_to_transfer", "max_amount"])
    for x in employees:
        employee_id = x["employee_id"]
        name = x["full_name"]
        if 'available_to_transfer' not in x['properties']:
            available_to_transfer = 0
        else:
            available_to_transfer = x['properties']['available_to_transfer']

        max_amount = 0
        if employee_id in max_transfers:
            max_amount = max_transfers[employee_id]
        
        employeeFile.writerow([name, available_to_transfer, max_amount])

#Get employees data from json file
def process_employee_transfers_offline():
    max_transfers = get_transfer_data_offline()
    employees = json.load(open("EmployeeRequest/data.json"))['data']
    employeeFile = csv.writer(open("Employees_offline.csv","w"))
    employeeFile.writerow(["name","available_to_transfer", "max_amount"])
    for x in employees:
        employee_id = x["employee_id"]
        name = x["full_name"]
        if 'available_to_transfer' not in x['properties']:
            available_to_transfer = 0
        else:
            available_to_transfer = x['properties']['available_to_transfer']

        max_amount = 0
        if employee_id in max_transfers:
            max_amount = max_transfers[employee_id]
        
        employeeFile.writerow([name, available_to_transfer, max_amount])

#Get data from server
def get_transfer_data():
    transferData = request_json('transfers')['data']
    transferDict = {}
    for e in transferData:
        empId = e['employee_id']
        if empId not in transferDict:
            transferDict[empId] = e['net_amount']
        else:
            transferDict[empId] = max(transferDict[e['employee_id']], e['net_amount'])
    return transferDict

#uploads data to database
def upload_employee_data():
    with open('EmployeeRequest/data.json') as employee_data:
        empoloyee_list = json.load(employee_data)['data']
        columns = [list(x.keys()) for x in empoloyee_list][0]
        print ("\ncolumn names:", columns)
    conn_str = "postgresql+psycopg2://root:docker@postgres_db:5432/postgres 2"
    con = psycopg2.connect(host = "DESKTOP-I0UCFQF",
    database='postgres',
    user='postgres',
    password='docker',
    port='5432'
    )

    con.close()
    with psycopg2.connect(host = "DESKTOP-I0UCFQF", database='postgres', user='postgres',password='docker', port='5432') as conn:

        with conn.cursor() as cur:
            with open('EmployeeRequest/transferdata.json') as transactions:
                transfer_data = json.load(transactions)['data']
            cur.execute(""" create table if not exists transactions_data_tab(
                employee_id text, completed_at timestamp, net_amount int, fee int) """)
            query_sql = """ insert into transactions_data_tab
                select * from json_populate_recordset(NULL::transactions_data_tab, %s) """
            cur.execute(query_sql, (json.dumps(transfer_data),))

#Getting transfer data from json file
def get_transfer_data_offline():
    transferData = json.load(open("EmployeeRequest/transferdata.json"))['data']
    transferDict = {}
    for e in transferData:
        empId = e['employee_id']
        if empId not in transferDict:
            transferDict[empId] = e['net_amount']
        else:
            transferDict[empId] = max(transferDict[e['employee_id']], e['net_amount'])
    return transferDict

upload_employee_data()
malformed_employees()
offline_malformed_employees()
process_employee_transfers()
process_employee_transfers_offline()
