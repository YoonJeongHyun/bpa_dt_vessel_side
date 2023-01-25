from utils.util import *
from utils.db_connection import *
from algorithms.plan_berth import *
from algorithms.vessel_monitoring import *
from algorithms.optimal_routing import *
from algorithms.table_for_visualization import *
import datetime
import os
import cx_Oracle
import pandas as pd
import warnings
import sys

warnings.filterwarnings(action="ignore")
import random

### SQL QUERY ###
date_period_1 = datetime.datetime.today()  # 오늘
date_period_2 = datetime.datetime.today() + datetime.timedelta(weeks=2)
# date_period_2 = datetime.datetime.today() + datetime.timedelta(weeks=3)
date_period_two_day = datetime.datetime.today() - datetime.timedelta(days=2)
date_period_1 = date_period_1.strftime("%Y%m%d")
date_period_2 = date_period_2.strftime("%Y%m%d")
date_period_two_day = date_period_two_day.strftime("%Y%m%d")
tos_plan_berth_sql = f"select * from TOS_PLAN_BERTH where ETA BETWEEN TO_DATE({date_period_1}, 'yyyy-mm-dd hh24:mi:ss') AND TO_DATE({date_period_2}, 'yyyy-mm-dd hh24:mi:ss')"
tos_vessel_sql = "select * from TOS_VESSEL"
ais_data_accumulated_sql_select = 'SELECT * FROM ais_data_accumulated'

# TODO 1. previous_ais_data_acculmulated

previous_ais_data_accumulated_sql_select = 'SELECT * FROM previous_ais_data_accumulated'

# column
previous_ais_data_accumulated_columns = [
    's_id', 'VSL_NM', 'tos_VSL_NM', 'MMSI', 'destination', 'navigation_info',
    'longitude', 'latitude', 'heading', 'speed', 'max_draught',
    'time_position_data_received', 'time_voyage_data_received', 'capacity',
    'time_difference', 'isnottoday', 'timestamp', 'predicted_ETA', 'tos_ETA',
    'tos_ETD', 'VOYAGE', 'DIS_QTY', 'LOAD_QTY', 'remained_distance',
    'estimated_fuel_consumption'
]

# DROP
previous_ais_data_accumulated_sql_drop = "DROP TABLE previous_ais_data_accumulated"
# CREATE
previous_ais_data_accumulated_sql_create = """CREATE TABLE previous_ais_data_accumulated (
s_id NUMBER not null,
VSL_NM VARCHAR2(40),
tos_VSL_NM VARCHAR2(40),
MMSI NUMBER,
destination VARCHAR2(40),
navigation_info VARCHAR2(40),
longitude NUMBER,
latitude NUMBER,
heading NUMBER,
speed NUMBER,
max_draught NUMBER,
time_position_data_received DATE,
time_voyage_data_received DATE,
capacity NUMBER,
time_difference VARCHAR2(40),
isnottoday VARCHAR2(40),
timestamp DATE,
predicted_ETA DATE,
tos_ETA DATE,
tos_ETD DATE,
VOYAGE VARCHAR2(40),
DIS_QTY NUMBER,
LOAD_QTY NUMBER,
remained_distance NUMBER,
estimated_fuel_consumption NUMBER)
"""
previous_ais_data_accumulated_sql_create = previous_ais_data_accumulated_sql_create.replace(
    "\n", "")
# sequence
previous_ais_data_accumulated_sql_sequence = "CREATE SEQUENCE seq_ais_acc start with 1 increment by 1 maxvalue 100000 cycle nocache"
# primary
previous_ais_data_accumulated_sql_primary = 'ALTER TABLE previous_ais_data_accumulated ADD CONSTRAINT previous_ais_primary PRIMARY KEY (VSL_NM)'
# grant
previous_ais_data_accumulated_sql_grant = 'GRANT ALL ON previous_ais_data_accumulated TO ispark'

# TODO 2. vessel_mmsi_list
# create
vessel_mmsi_list_sql_create = """CREATE TABLE vessel_mmsi_list (
VSL_NM VARCHAR2(40)
mmsi_no NUMBER,
IMO NUMBER,
TEU NUMBER
)
"""
vessel_mmsi_list_sql_create = vessel_mmsi_list_sql_create.replace(
    "\n", "")

# TODO 3. ais_data_accumulated

ais_data_accumulated_sql_select = 'SELECT * FROM ais_data_accumulated'

# DROP
ais_data_accumulated_sql_drop = "DROP TABLE ais_data_accumulated"
# CREATE
ais_data_accumulated_sql_create = """CREATE TABLE ais_data_accumulated (
s_id NUMBER not null,
VSL_NM VARCHAR2(40),
tos_VSL_NM VARCHAR2(40),
MMSI NUMBER,
destination VARCHAR2(40),
navigation_info VARCHAR2(40),
longitude NUMBER,
latitude NUMBER,
heading NUMBER,
speed NUMBER,
max_draught NUMBER,
time_position_data_received DATE,
time_voyage_data_received DATE,
capacity NUMBER,
time_difference VARCHAR2(40),
isnottoday VARCHAR2(40),
timestamp DATE,
predicted_ETA DATE,
tos_ETA DATE,
tos_ETD DATE,
VOYAGE VARCHAR2(40),
DIS_QTY NUMBER,
LOAD_QTY NUMBER,
remained_distance NUMBER,
estimated_fuel_consumption NUMBER)
"""
ais_data_accumulated_sql_create = ais_data_accumulated_sql_create.replace(
    "\n", "")
# sequence
ais_data_accumulated_sql_sequence = "CREATE SEQUENCE seq_ais_acc start with 1 increment by 1 maxvalue 100000 cycle nocache"
# primary
ais_data_accumulated_sql_primary = 'ALTER TABLE ais_data_accumulated ADD CONSTRAINT ais_primary PRIMARY KEY (s_id, VSL_NM)'
# grant
ais_data_accumulated_sql_grant = 'GRANT ALL ON ais_data_accumulated TO ispark'

ais_data_accumulated_columns = [
    's_id', 'VSL_NM', 'tos_VSL_NM', 'MMSI', 'destination', 'navigation_info',
    'longitude', 'latitude', 'heading', 'speed', 'max_draught',
    'time_position_data_received', 'time_voyage_data_received', 'capacity',
    'time_difference', 'isnottoday', 'timestamp', 'predicted_ETA', 'tos_ETA',
    'tos_ETD', 'VOYAGE', 'DIS_QTY', 'LOAD_QTY', 'remained_distance',
    'estimated_fuel_consumption'
]

# TODO 4. tracking_vsl_nm
tracking_vsl_nm_list_sql_select = 'SELECT * FROM tracking_vsl_nm_list'

tracking_vsl_nm_list_sql_create = """CREATE TABLE tracking_vsl_nm_list (
TOS_VSL_NM VARCHAR2(40),
DESTINATION VARCHAR2(40))
"""
tracking_vsl_nm_list_sql_create = tracking_vsl_nm_list_sql_create.replace(
    "\n", "")

tracking_vsl_columns = ['TOS_VSL_NM', 'DESTINATION']

# TODO 5 vessel_heading_to_busan_tracking
# # DROP
# # vessel_heading_to_busan_tracking_sql_drop = "DROP TABLE vessel_heading_to_busan_tracking"
# CREATE
vessel_heading_to_busan_tracking_sql_create = """CREATE TABLE vessel_heading_to_busan_tracking (
s_id NUMBER not null,
VSL_NM VARCHAR2(40),
tos_VSL_NM VARCHAR2(40),
MMSI NUMBER,
destination VARCHAR2(40),
navigation_info VARCHAR2(40),
longitude NUMBER,
latitude NUMBER,
heading NUMBER,
speed NUMBER,
max_draught NUMBER,
time_position_data_received DATE,
time_voyage_data_received DATE,
capacity NUMBER,
time_difference VARCHAR2(40),
isnottoday VARCHAR2(40),
timestamp DATE,
predicted_ETA DATE,
tos_ETA DATE,
tos_ETD DATE,
VOYAGE VARCHAR2(40),
DIS_QTY NUMBER,
LOAD_QTY NUMBER,
remained_distance NUMBER,
estimated_fuel_consumption NUMBER)
"""
vessel_heading_to_busan_tracking_create = vessel_heading_to_busan_tracking_sql_create.replace(
    "\n", "")

# TODO 6.vessel_voyage_report

vessel_voyage_report_sql_create = """CREATE TABLE vessel_voyage_report (
VSL_NM VARCHAR2(40),
tos_VSL_NM VARCHAR2(40),
MMSI NUMBER,
VOYAGE VARCHAR2(40),
destination VARCHAR2(40),
departing_longitude NUMBER,
departing_latitude NUMBER,
mean_speed NUMBER,
timestamp DATE,
tos_ETA DATE,
tos_ETD DATE,
capacity NUMBER,
overall_distance NUMBER,
final_estimated_fuel_consumption NUMBER,
FINAL_CO2_EMISSION NUMBER,
PREDICTED_ETA DATE
)
"""
vessel_voyage_report_sql_create = vessel_voyage_report_sql_create.replace(
    "\n", "")
vessel_voyage_report_columns = [
    'VSL_NM', 'tos_VSL_NM', 'MMSI', 'VOYAGE', 'destination',
    'departing_longitude', 'departing_latitude', 'mean_speed', 'timestamp',
    'tos_ETA', 'tos_ETD', 'capacity', 'overall_distance',
    'final_estimated_fuel_consumption', 'FINAL_CO2_EMISSION', 'PREDICTED_ETA'
]

vessel_voyage_report_columns = list(map(upper, vessel_voyage_report_columns))

# TODO 6 ais_data_accumulated_500
# DROP
ais_data_accumulated_500_sql_drop = "DROP TABLE ais_data_accumulated_500"
# CREATE
ais_data_accumulated_500_sql_create = """CREATE TABLE ais_data_accumulated_500 (
s_id NUMBER not null,
VSL_NM VARCHAR2(40),
excel_vessel_name VARCHAR2(40),
MMSI NUMBER,
destination VARCHAR2(40),
navigation_info VARCHAR2(40),
longitude NUMBER,
latitude NUMBER,
heading NUMBER,
speed NUMBER,
max_draught NUMBER,
time_position_data_received DATE,
time_voyage_data_received DATE,
time_difference VARCHAR2(40),
isnottoday VARCHAR2(40),
timestamp DATE
)
"""
ais_data_accumulated_500_sql_create = ais_data_accumulated_500_sql_create.replace(
    "\n", "")
# sequence
ais_data_accumulated_500_sql_sequence = "CREATE SEQUENCE seq_ais_acc_500 start with 1 increment by 1 maxvalue 100000 cycle nocache"
# primary
ais_data_accumulated_500_sql_primary = 'ALTER TABLE ais_data_accumulated_500 ADD CONSTRAINT ais_500_primary PRIMARY KEY (s_id, VSL_NM)'
# grant
ais_data_accumulated_500_sql_grant = 'GRANT ALL ON ais_data_accumulated_500 TO ispark'

ais_data_accumulated_500_columns = [
    's_id', 'VSL_NM', 'excel_vessel_name', 'MMSI', 'destination',
    'navigation_info', 'longitude', 'latitude', 'heading', 'speed',
    'max_draught', 'time_position_data_received', 'time_voyage_data_received',
    'time_difference', 'isnottoday', 'timestamp'
]

# TODO : 7. 최적항로
# 2. 선박 예상도칙시간 산출
vssl_optimal_routing_columns = [
    'OR_ID', "VSL_CD", 'CLSGN', 'VSSL_MMSI', 'VSSL_ENG_NM', 'YR', 'SERNO',
    'IBOBPRT_SE', 'CRRNT_TIME', 'CRRNT_LAT', 'CRRNT_LON', 'CRRNT_HDG', 'TIME1',
    'LAT1', 'LON1', 'HDG1', 'TIME2', 'LAT2', 'LON2', 'HDG2', 'TIME3', 'LAT3',
    'LON3', 'HDG3', 'TIME4', 'LAT4', 'LON4', 'HDG4', 'TIME5', 'LAT5', 'LON5',
    'HDG5', 'TIME6', 'LAT6', 'LON6', 'HDG6', 'TIME7', 'LAT7', 'LON7', 'HDG7',
    'TIME8', 'LAT8', 'LON8', 'HDG8', 'TIME9', 'LAT9', 'LON9', 'HDG9', 'TIME10',
    'LAT10', 'LON10', 'HDG10', 'TIME11', 'LAT11', 'LON11', 'HDG11', 'TIME12',
    'LAT12', 'LON12', 'HDG12', 'TIME13', 'LAT13', 'LON13', 'HDG13', 'TIME14',
    'LAT14', 'LON14', 'HDG14', 'TIME15', 'LAT15', 'LON15', 'HDG15', 'TIME16',
    'LAT16', 'LON16', 'HDG16', 'TIME17', 'LAT17', 'LON17', 'HDG17', 'TIME18',
    'LAT18', 'LON18', 'HDG18', 'TIME19', 'LAT19', 'LON19', 'HDG19', 'TIME20',
    'LAT20', 'LON20', 'HDG20', 'REMAINED_DISTANCE', 'ETA_PORT_TOS',
    'ETA_ANALYSIS', 'ATA', 'ERROR', 'MEAN_SPEED', 'VSSL_FOC', 'VSSL_CO2',
    'DEPARTING_LONGITUDE', 'DEPARTING_LATITUDE', 'ARRIVING_ROUTE_LAT1',
    'ARRIVING_ROUTE_LON1', 'ARRIVING_ROUTE_LAT2', 'ARRIVING_ROUTE_LON2',
    'ARRIVING_ROUTE_LAT3', 'ARRIVING_ROUTE_LON3', 'ARRIVING_ROUTE_LAT4',
    'ARRIVING_ROUTE_LON4', 'ARRIVING_ROUTE_LAT5', 'ARRIVING_ROUTE_LON5',
    'ARRIVING_ROUTE_LAT6', 'ARRIVING_ROUTE_LON6', 'ARRIVING_ROUTE_LAT7',
    'ARRIVING_ROUTE_LON7', 'ARRIVING_ROUTE_LAT8', 'ARRIVING_ROUTE_LON8',
    'ARRIVING_ROUTE_LAT9', 'ARRIVING_ROUTE_LON9', 'ARRIVING_ROUTE_LAT10',
    'ARRIVING_ROUTE_LON10', 'ARRIVING_ROUTE_LAT11', 'ARRIVING_ROUTE_LON11',
    'ARRIVING_ROUTE_LAT12', 'ARRIVING_ROUTE_LON12', 'ARRIVING_ROUTE_LAT13',
    'ARRIVING_ROUTE_LON13', 'ARRIVING_ROUTE_LAT14', 'ARRIVING_ROUTE_LON14',
    'ARRIVING_ROUTE_LAT15', 'ARRIVING_ROUTE_LON15', 'ARRIVING_ROUTE_LAT16',
    'ARRIVING_ROUTE_LON16', 'ARRIVING_ROUTE_LAT17', 'ARRIVING_ROUTE_LON17',
    'ARRIVING_ROUTE_LAT18', 'ARRIVING_ROUTE_LON18', 'ARRIVING_ROUTE_LAT19',
    'ARRIVING_ROUTE_LON19', 'ARRIVING_ROUTE_LAT20', 'ARRIVING_ROUTE_LON20'
]

vssl_optimal_routing_sql_create = """
create table VSSL_OPTIMAL_ROUTING
 (OR_ID NUMBER not null,
VSL_CD VARCHAR2(40),
CLSGN VARCHAR2(40),
VSSL_MMSI NUMBER,
VSSL_ENG_NM VARCHAR2(40),
YR NUMBER,
SERNO NUMBER,
IBOBPRT_SE NUMBER,
CRRNT_TIME DATE,
CRRNT_LAT NUMBER,
CRRNT_LON NUMBER,
CRRNT_HDG NUMBER,
TIME1 DATE,
LAT1 NUMBER,
LON1 NUMBER,
HDG1 NUMBER,
TIME2 DATE,
LAT2 NUMBER,
LON2 NUMBER,
HDG2 NUMBER,
TIME3 DATE,
LAT3 NUMBER,
LON3 NUMBER,
HDG3 NUMBER,
TIME4 DATE,
LAT4 NUMBER,
LON4 NUMBER,
HDG4 NUMBER,
TIME5 DATE,
LAT5 NUMBER,
LON5 NUMBER,
HDG5 NUMBER,
TIME6 DATE,
LAT6 NUMBER,
LON6 NUMBER,
HDG6 NUMBER,
TIME7 DATE,
LAT7 NUMBER,
LON7 NUMBER,
HDG7 NUMBER,
TIME8 DATE,
LAT8 NUMBER,
LON8 NUMBER,
HDG8 NUMBER,
TIME9 DATE,
LAT9 NUMBER,
LON9 NUMBER,
HDG9 NUMBER,
TIME10 DATE,
LAT10 NUMBER,
LON10 NUMBER,
HDG10 NUMBER,
TIME11 DATE,
LAT11 NUMBER,
LON11 NUMBER,
HDG11 NUMBER,
TIME12 DATE,
LAT12 NUMBER,
LON12 NUMBER,
HDG12 NUMBER,
TIME13 DATE,
LAT13 NUMBER,
LON13 NUMBER,
HDG13 NUMBER,
TIME14 DATE,
LAT14 NUMBER,
LON14 NUMBER,
HDG14 NUMBER,
TIME15 DATE,
LAT15 NUMBER,
LON15 NUMBER,
HDG15 NUMBER,
TIME16 DATE,
LAT16 NUMBER,
LON16 NUMBER,
HDG16 NUMBER,
TIME17 DATE,
LAT17 NUMBER,
LON17 NUMBER,
HDG17 NUMBER,
TIME18 DATE,
LAT18 NUMBER,
LON18 NUMBER,
HDG18 NUMBER,
TIME19 DATE,
LAT19 NUMBER,
LON19 NUMBER,
HDG19 NUMBER,
TIME20 DATE,
LAT20 NUMBER,
LON20 NUMBER,
HDG20 NUMBER,
REMAINED_DISTANCE NUMBER,
ETA_PORT_TOS DATE,
ETA_ANALYSIS DATE,
ATA DATE,
ERROR NUMBER,
MEAN_SPEED NUMBER,
VSSL_FOC NUMBER,
VSSL_CO2 NUMBER,
DEPARTING_LONGITUDE NUMBER,
DEPARTING_LATITUDE NUMBER,
ARRIVING_ROUTE_LAT1 NUMBER,
ARRIVING_ROUTE_LON1 NUMBER,
ARRIVING_ROUTE_LAT2 NUMBER,
ARRIVING_ROUTE_LON2 NUMBER,
ARRIVING_ROUTE_LAT3 NUMBER,
ARRIVING_ROUTE_LON3 NUMBER,
ARRIVING_ROUTE_LAT4 NUMBER,
ARRIVING_ROUTE_LON4 NUMBER,
ARRIVING_ROUTE_LAT5 NUMBER,
ARRIVING_ROUTE_LON5 NUMBER,
ARRIVING_ROUTE_LAT6 NUMBER,
ARRIVING_ROUTE_LON6 NUMBER,
ARRIVING_ROUTE_LAT7 NUMBER,
ARRIVING_ROUTE_LON7 NUMBER,
ARRIVING_ROUTE_LAT8 NUMBER,
ARRIVING_ROUTE_LON8 NUMBER,
ARRIVING_ROUTE_LAT9 NUMBER,
ARRIVING_ROUTE_LON9 NUMBER,
ARRIVING_ROUTE_LAT10 NUMBER,
ARRIVING_ROUTE_LON10 NUMBER,
ARRIVING_ROUTE_LAT11 NUMBER,
ARRIVING_ROUTE_LON11 NUMBER,
ARRIVING_ROUTE_LAT12 NUMBER,
ARRIVING_ROUTE_LON12 NUMBER,
ARRIVING_ROUTE_LAT13 NUMBER,
ARRIVING_ROUTE_LON13 NUMBER,
ARRIVING_ROUTE_LAT14 NUMBER,
ARRIVING_ROUTE_LON14 NUMBER,
ARRIVING_ROUTE_LAT15 NUMBER,
ARRIVING_ROUTE_LON15 NUMBER,
ARRIVING_ROUTE_LAT16 NUMBER,
ARRIVING_ROUTE_LON16 NUMBER,
ARRIVING_ROUTE_LAT17 NUMBER,
ARRIVING_ROUTE_LON17 NUMBER,
ARRIVING_ROUTE_LAT18 NUMBER,
ARRIVING_ROUTE_LON18 NUMBER,
ARRIVING_ROUTE_LAT19 NUMBER,
ARRIVING_ROUTE_LON19 NUMBER,
ARRIVING_ROUTE_LAT20 NUMBER,
ARRIVING_ROUTE_LON20 NUMBER
)
"""
vssl_optimal_routing_sql_create = vssl_optimal_routing_sql_create.replace(
    "\n", '')
vssl_optimal_routing_sql_create = vssl_optimal_routing_sql_create.replace(
    "\t", '')

vssl_optimal_routing_sql_sequence = """
CREATE SEQUENCE seq_vssl_optimal_routing start with 1 increment by 1 maxvalue 1000000000 cycle nocache
"""

vssl_optimal_routing_sql_drop = """
DROP TABLE VSSL_OPTIMAL_ROUTING
"""
vssl_optimal_routing_sql_primary = '''
ALTER TABLE VSSL_OPTIMAL_ROUTING ADD CONSTRAINT vessel_op_routing_pri PRIMARY KEY (OR_ID)
'''
vssl_optimal_routing_sql_grant = 'GRANT ALL ON VSSL_OPTIMAL_ROUTING TO ispark'

## TODO : 8. TUG_STATIC_AREA
tug_static_area_sql_select = "select * from TUG_STATIC_AREA"

## TODO : 9. plan_berth

# 3번 최적 선석 계획
# 1
original_tos_plan_berth_a_columns = [
    'a_id', 'VSL_CD', 'VOYAGE', 'YR', 'SER_NO', 'VSL_NM', 'PTNR_CODE',
    'ALONG_SIDE', 'BERTH_A', 'FROM_BITT_A', 'TO_BITT_A', 'ETA', 'ETD',
    'DIS_QTY', 'LOAD_QTY', 'LOA', "WIDTH", 'timestamp'
]

original_tos_plan_berth_a_sql_create = """
CREATE TABLE original_tos_plan_berth_a (
a_id NUMBER not null,
VSL_CD VARCHAR2(40),
VOYAGE VARCHAR2(40),
YR NUMBER,
SER_NO NUMBER,
VSL_NM VARCHAR2(40),
PTNR_CODE VARCHAR2(40),
ALONG_SIDE VARCHAR2(40),
BERTH_A VARCHAR2(40),
FROM_BITT_A VARCHAR2(40),
TO_BITT_A VARCHAR2(40),
ETA DATE,
ETD DATE,
DIS_QTY NUMBER,
LOAD_QTY NUMBER,
LOA NUMBER,
WIDTH NUMBER,
timestamp DATE
)
"""

original_tos_plan_berth_a_sql_create = original_tos_plan_berth_a_sql_create.replace(
    "\n", '')

original_tos_plan_berth_a_sql_sequence = """
CREATE SEQUENCE seq_berth_a start with 1 increment by 1 maxvalue 1000000000 cycle nocache
"""

original_tos_plan_berth_a_sql_drop = """
DROP TABLE original_tos_plan_berth_a
"""
original_tos_plan_berth_a_sql_primary = '''
ALTER TABLE original_tos_plan_berth_a ADD CONSTRAINT berth_a_pri_ PRIMARY KEY (a_id)

'''

original_tos_plan_berth_a_sql_grant = 'GRANT ALL ON original_tos_plan_berth_a TO ispark'

# 2
terminal_optimal_plan_berth_b_columns = [
    'b_id', 'VSL_CD', 'VOYAGE', 'YR', 'SER_NO', 'VSL_NM', 'PTNR_CODE',
    'ALONG_SIDE', 'BERTH_B', 'FROM_BITT_B', 'TO_BITT_B', 'RTA', 'RTD',
    'DIS_QTY', 'LOAD_QTY', 'LOA', "WIDTH", 'timestamp'
]

terminal_optimal_plan_berth_b_sql_create = """
CREATE TABLE terminal_optimal_plan_berth_b (
b_id NUMBER not null,
VSL_CD VARCHAR2(40),
VOYAGE VARCHAR2(40),
YR NUMBER,
SER_NO NUMBER,
VSL_NM VARCHAR2(40),
PTNR_CODE VARCHAR2(40),
ALONG_SIDE VARCHAR2(40),
BERTH_B VARCHAR2(40),
FROM_BITT_B VARCHAR2(40),
TO_BITT_B VARCHAR2(40),
RTA DATE,
RTD DATE,
DIS_QTY NUMBER,
LOAD_QTY NUMBER,
LOA NUMBER,
WIDTH NUMBER,
timestamp DATE
)
"""

terminal_optimal_plan_berth_b_sql_create = terminal_optimal_plan_berth_b_sql_create.replace(
    "\n", '')

terminal_optimal_plan_berth_b_sql_sequence = """
CREATE SEQUENCE seq_berth_b start with 1 increment by 1 maxvalue 10000000 cycle nocache
"""

terminal_optimal_plan_berth_b_sql_drop = """
DROP TABLE terminal_optimal_plan_berth_b
"""

terminal_optimal_plan_berth_b_sql_primary = '''
ALTER TABLE terminal_optimal_plan_berth_b ADD CONSTRAINT berth_b_pri PRIMARY KEY (b_id)
'''

terminal_optimal_plan_berth_b_sql_grant = 'GRANT ALL ON terminal_optimal_plan_berth_b TO ispark'

# 3
mutual_optimal_plan_berth_c_columns = [
    'c_id', 'VSL_CD', 'VOYAGE', 'YR', 'SER_NO', 'VSL_NM', 'PTNR_CODE',
    'ALONG_SIDE', 'BERTH_C', 'FROM_BITT_C', 'TO_BITT_C', 'PTA', 'PTD',
    'DIS_QTY', 'LOAD_QTY', 'LOA', 'WIDTH', 'timestamp'
]

mutual_optimal_plan_berth_c_sql_create = """
CREATE TABLE mutual_optimal_plan_berth_c (
c_id NUMBER not null,
VSL_CD VARCHAR2(40),
VOYAGE VARCHAR2(40),
YR NUMBER,
SER_NO NUMBER,
VSL_NM VARCHAR2(40),
PTNR_CODE VARCHAR2(40),
ALONG_SIDE VARCHAR2(40),
BERTH_C VARCHAR2(40),
FROM_BITT_C VARCHAR2(40),
TO_BITT_C VARCHAR2(40),
PTA DATE,
PTD DATE,
DIS_QTY NUMBER,
LOAD_QTY NUMBER,
LOA NUMBER,
WIDTH NUMBER,
timestamp DATE
)
"""

mutual_optimal_plan_berth_c_sql_create = mutual_optimal_plan_berth_c_sql_create.replace(
    "\n", '')

mutual_optimal_plan_berth_c_sql_sequence = """
CREATE SEQUENCE seq_berth_c start with 1 increment by 1 maxvalue 10000000 cycle nocache
"""

mutual_optimal_plan_berth_c_sql_drop = """
DROP TABLE mutual_optimal_plan_berth_c
"""

mutual_optimal_plan_berth_c_sql_primary = '''
ALTER TABLE mutual_optimal_plan_berth_c ADD CONSTRAINT berth_c_pri PRIMARY KEY (c_id)
'''

mutual_optimal_plan_berth_c_sql_grant = 'GRANT ALL ON mutual_optimal_plan_berth_c TO ispark'

# TODO : 9. TUG_ALLOCATION
tug_allocation_sql_create = """
CREATE TABLE tug_allocation (
TA_ID NUMBER,
VSSL_ENG_NM VARCHAR2(40),
BERTH VARCHAR2(40),
et NUMBER,
TUG_START_TIME DATE,
TUG_FIN_DATE DATE,
TUG1_NAME VARCHAR2(40),
TUG2_NAME VARCHAR2(40),
TUG3_NAME VARCHAR2(40),
cost NUMBER,
CLSGN VARCHAR2(40)
)
"""

tug_allocation_sql_create = tug_allocation_sql_create.replace(
    "\n", '')

tug_allocation_sql_sequence = """
CREATE SEQUENCE seq_tug_allo start with 1 increment by 1 maxvalue 10000000 cycle nocache
"""

tug_allocation_sql_drop = """
DROP TABLE tug_allocation
"""

tug_allocation_sql_primary = '''
ALTER TABLE tug_allocation ADD CONSTRAINT tug_allocation_pri PRIMARY KEY (VSSL_KEY)
'''

tug_allocation_sql_grant = 'GRANT ALL ON tug_allocation TO ispark'

tug_allocation_columns = ['TA_ID', 'VSSL_ENG_NM', 'BERTH', 'et', 'TUG_START_TIME', 'TUG_FIN_DATE', 'TUG1_NAME',
                          'TUG2_NAME', 'TUG3_NAME', 'cost', 'CLSGN']

# TODO : 10. monitoring

# arrival_monitoring
arrival_monitoring_sql_drop = 'DROP TABLE ARRIVAL_MONITORING'

arrival_monitoring_sql_create = """CREATE TABLE ARRIVAL_MONITORING (
VSL_NM VARCHAR2(40),
VOYAGE VARCHAR2(40),
REMAINED_DISTANCE NUMBER,
FUEL_CONSUMPTION NUMBER,
CO2_EMISSION NUMBER,
BERTH VARCHAR2(40),
MEAN_SPEED NUMBER,
TOS_ETA DATE,
ETA DATE,
RTA DATE,
PTA DATE,
STATUS VARCHAR2(40),
TIMESTAMP DATE
)
"""
arrival_monitoring_sql_create = arrival_monitoring_sql_create.replace("\n", "")

arrival_monitoring_sql_grant = 'GRANT ALL ON ARRIVAL_MONITORING TO ispark'

arrival_monitoring_columns = [
    'VSL_NM', 'VOYAGE', 'REMAINED_DISTANCE', 'FUEL_CONSUMPTION',
    'CO2_EMISSION', 'BERTH', 'MEAN_SPEED', 'TOS_ETA', 'ETA', 'RTA', 'PTA',
    'STATUS', 'TIMESTAMP'
]

# departure monitoring
departure_monitoring_sql_drop = 'DROP TABLE DEPARTURE_MONITORING'

departure_monitoring_sql_create = """CREATE TABLE DEPARTURE_MONITORING (
VSL_NM VARCHAR2(40),
VOYAGE VARCHAR2(40),
DWELL_TIME VARCHAR2(40),
BERTH VARCHAR2(40),
TOS_ETD DATE,
ETD DATE,
STATUS VARCHAR2(40),
TIMESTAMP DATE
)
"""
departure_monitoring_sql_create = departure_monitoring_sql_create.replace(
    "\n", "")

departure_monitoring_sql_grant = 'GRANT ALL ON DEPARTURE_MONITORING TO ispark'

departure_monitoring_columns = [
   'VSL_NM', 'VOYAGE', 'DWELL_TIME', 'BERTH', 'TOS_ETD', 'ETD',
    'STATUS', 'TIMESTAMP'
]

date_period_2days_after = datetime.datetime.today() + datetime.timedelta(days=2)
date_period_2days_after = date_period_2days_after.strftime("%Y%m%d")

date_period_1days_prior = datetime.datetime.today() - datetime.timedelta(days=1)
date_period_7days_prior = datetime.datetime.today() - datetime.timedelta(days=7)
date_period_1days_prior = date_period_1days_prior.strftime("%Y%m%d")
date_period_7days_prior = date_period_7days_prior.strftime("%Y%m%d")
vssl_optimal_routing_sql_select = "select * from vssl_optimal_routing"
vessel_voyage_report_7days_sql_select = f"select * from vessel_voyage_report where TIMESTAMP BETWEEN TO_DATE({date_period_7days_prior}, 'yyyy-mm-dd hh24:mi:ss') AND TO_DATE({date_period_2days_after}, 'yyyy-mm-dd hh24:mi:ss')"
option_a_select = "select * from original_tos_plan_berth_a"
option_b_select = "select * from terminal_optimal_plan_berth_b"
option_c_select = "select * from mutual_optimal_plan_berth_c"
ais_accumulated_select = "select * from AIS_DATA_ACCUMULATED"

tos_plan_berth_atb_sql_select = f"select * from TOS_PLAN_BERTH where ATB BETWEEN TO_DATE({date_period_1days_prior}, 'yyyy-mm-dd hh24:mi:ss') AND TO_DATE({date_period_2days_after}, 'yyyy-mm-dd hh24:mi:ss')"
tos_plan_berth_atd_sql_select = f"select * from TOS_PLAN_BERTH where ATD BETWEEN TO_DATE({date_period_1days_prior}, 'yyyy-mm-dd hh24:mi:ss') AND TO_DATE({date_period_2days_after}, 'yyyy-mm-dd hh24:mi:ss')"

tug_boat_static_sql = "select * from TUG_BOAT_STATIC"
vessel_static_sql = "select * from VESSEL_STATIC"


def main(mode="outside"):
    PNIT_anchorage = (34.9696, 128.81945)

    if mode == "pnit" or mode == "PNIT":
        tsb_db = cx_Oracle.connect("tsb", "tsb00", "192.168.16.21:1521/BPADTDB")
        sju_db = cx_Oracle.connect("sju", "sju05", "192.168.16.21:1521/BPADTDB")
        pisces_url = "http://192.168.16.51/v1/estimateWorkTime"
    else:
        tsb_db = cx_Oracle.connect("tsb", "tsb00", "61.76.45.152:1521/BPADTDB")
        sju_db = cx_Oracle.connect("sju", "sju05", "61.76.45.152:1521/BPADTDB")
        pisces_url = 'http://simberth.piscesoft.com/v1/estimateWorkTime'

    tos_plan_berth_df = pd.read_sql(tos_plan_berth_sql, tsb_db)
    now = datetime.datetime.now()
    tos_plan_berth_df["timestamp"] = now

    tos_vessel = pd.read_sql(tos_vessel_sql, tsb_db)
    tuglist = pd.read_sql(tug_boat_static_sql, sju_db)
    vessel_data = pd.read_sql(vessel_static_sql, sju_db)
    # tsb_db.close()

    ais_data_accumulated = pd.read_sql(ais_data_accumulated_sql_select, sju_db)
    db_write(sju_db,
             previous_ais_data_accumulated_sql_drop,
             previous_ais_data_accumulated_sql_create,
             previous_ais_data_accumulated_sql_sequence,
             previous_ais_data_accumulated_sql_primary,
             previous_ais_data_accumulated_sql_grant,
             previous_ais_data_accumulated_columns,
             ais_data_accumulated,
             'previous_ais_data_accumulated')

    # orbcomn_fleet_delete()

    observing_mmsi_list, mmsi_vessel_df, no_mmsi_list = search_for_vessels(tos_plan_berth_df, tos_vessel)
    data_dict = ais_monitoring(observing_mmsi_list, mmsi_vessel_df=mmsi_vessel_df,
                               tos_plan_berth_df=tos_plan_berth_df)
    busan_arriving_vessels = make_busan_arriving_vessels_df(data_dict, PNIT_anchorage)
    busan_arriving_vessels = busan_arriving_vessels[ais_data_accumulated_columns]
    busan_arriving_vessels.reset_index(drop=True, inplace=True)

    db_write(sju_db,
             ais_data_accumulated_sql_drop,
             ais_data_accumulated_sql_create,
             ais_data_accumulated_sql_sequence,
             ais_data_accumulated_sql_primary,
             ais_data_accumulated_sql_grant,
             ais_data_accumulated_columns,
             busan_arriving_vessels,
             'ais_data_accumulated')

    previous_ais_data_accumulated = pd.read_sql(previous_ais_data_accumulated_sql_select, sju_db)
    ais_data_accumulated = pd.read_sql(ais_data_accumulated_sql_select, sju_db)
    tracking_vsl_nm_list_df = pd.read_sql(tracking_vsl_nm_list_sql_select, sju_db)

    tracking_vsl_df = tracking_vessels_monitoring(tracking_vsl_nm_list_df, ais_data_accumulated,
                                                  previous_ais_data_accumulated)
    tracking_vsl_df = tracking_vsl_df[tracking_vsl_columns]
    db_write(sju_db,
             None,
             tracking_vsl_nm_list_sql_create,
             None,
             None,
             None,
             tracking_vsl_columns,
             tracking_vsl_df,
             'tracking_vsl_nm_list')

    tracking_vsl_nm_list_df = pd.read_sql(tracking_vsl_nm_list_sql_select, sju_db)
    tracking_vsl_nm_list_df.drop_duplicates(inplace=True)

    tracking_idx_list = []
    for idx in tracking_vsl_df.index:
        vsl_nm = tracking_vsl_df.loc[idx, "TOS_VSL_NM"]
        index = ais_data_accumulated.query(f"TOS_VSL_NM == '{vsl_nm}'").index
        try:
            tracking_idx_list.append(index[0])
        except:
            pass

    vessel_heading_to_busan_tracking_df = ais_data_accumulated.loc[tracking_idx_list, :]
    vessel_heading_to_busan_tracking_df.reset_index(inplace=True, drop=True)
    vessel_heading_to_busan_tracking_df_columns = vessel_heading_to_busan_tracking_df.columns

    db_write(sju_db,
             None,
             vessel_heading_to_busan_tracking_sql_create,
             None,
             None,
             None,
             vessel_heading_to_busan_tracking_df_columns,
             vessel_heading_to_busan_tracking_df,
             'vessel_heading_to_busan_tracking')

    vessels_anchoring_at_busan_list, vessel_voyage_report = check_vessels_in_boundary_of_pnit(ais_data_accumulated,
                                                                                              sju_db)
    vessel_voyage_report = vessel_voyage_report[vessel_voyage_report_columns]

    db_write(sju_db,
             None,
             vessel_voyage_report_sql_create,
             None,
             None,
             None,
             vessel_voyage_report_columns,
             vessel_voyage_report,
             'vessel_voyage_report')

    cursor = sju_db.cursor()

    for vsl_nm in vessels_anchoring_at_busan_list:
        vessel_heading_to_busan_tracking_sql_delete = f"DELETE FROM vessel_heading_to_busan_tracking WHERE TOS_VSL_NM = '{vsl_nm}'"
        cursor.execute(vessel_heading_to_busan_tracking_sql_delete)
        tracking_vsl_nm_list_sql_delete = f"DELETE FROM tracking_vsl_nm_list WHERE TOS_VSL_NM = '{vsl_nm}'"
        cursor.execute(tracking_vsl_nm_list_sql_delete)
        sju_db.commit()

    # # 남은 선박 모니터링
    # vessel_500_df = pd.read_csv("vessel_list_3.csv")
    # # idx_rdm_400 = random.sample(list(vessel_500_df.index), 400)
    # idx_rdm_400 = [
    #     173, 267, 147, 212, 346, 256, 174, 236, 217, 244, 301, 261, 145, 336, 79,
    #     420, 101, 111, 352, 391, 338, 406, 139, 218, 73, 126, 121, 477, 308, 353,
    #     38, 5, 377, 260, 48, 172, 168, 268, 347, 439, 144, 209, 355, 80, 367, 354,
    #     46, 421, 344, 361, 416, 375, 148, 396, 65, 43, 458, 425, 170, 71, 446, 74,
    #     85, 248, 6, 56, 493, 299, 334, 395, 463, 67, 176, 146, 40, 210, 358, 488,
    #     239, 161, 320, 279, 440, 92, 298, 296, 119, 470, 394, 339, 277, 221, 434,
    #     91, 290, 429, 227, 300, 503, 57, 12, 27, 107, 175, 326, 284, 486, 130, 419,
    #     474, 34, 115, 489, 102, 399, 224, 186, 132, 374, 472, 351, 166, 131, 204,
    #     392, 494, 368, 82, 484, 319, 203, 10, 370, 78, 140, 156, 497, 72, 356, 199,
    #     435, 293, 325, 98, 371, 87, 438, 253, 70, 155, 310, 7, 94, 116, 291, 272,
    #     288, 24, 29, 485, 51, 481, 357, 60, 201, 289, 498, 249, 15, 84, 449, 13,
    #     403, 189, 245, 330, 479, 124, 192, 402, 405, 200, 182, 195, 398, 133, 464,
    #     237, 331, 169, 444, 262, 3, 468, 234, 328, 167, 491, 81, 163, 304, 473,
    #     411, 247, 54, 322, 99, 492, 259, 187, 32, 49, 450, 362, 243, 41, 363, 274,
    #     321, 14, 342, 106, 225, 21, 164, 427, 379, 185, 53, 76, 77, 448, 219, 235,
    #     20, 280, 333, 44, 180, 309, 16, 315, 442, 109, 433, 418, 469, 424, 378, 45,
    #     150, 93, 343, 202, 417, 95, 281, 97, 258, 105, 263, 376, 90, 137, 64, 25,
    #     487, 276, 283, 265, 18, 359, 461, 360, 273, 385, 372, 350, 55, 129, 190,
    #     162, 407, 381, 151, 136, 311, 306, 4, 135, 428, 495, 165, 455, 345, 242,
    #     63, 451, 108, 39
    # ]
    # vessel_500_df.loc[idx_rdm_400, :].sort_index().reset_index(inplace=True, drop=True)
    # vessel_500_df = vessel_500_df.loc[idx_rdm_400, :].sort_index().reset_index(drop=True)
    # vessel_500_MMSI_list = vessel_500_df["MMSI_NO"].values
    # data_dict = ais_monitoring(vessel_500_MMSI_list, mode="ais_500")
    # vessel_500_orbcomn_df = make_vessel_500_orbcomn_df(data_dict)
    # vessel_500_orbcomn_df = vessel_500_orbcomn_df[ais_data_accumulated_500_columns]
    # vessel_500_orbcomn_df.reset_index(drop=True, inplace=True)
    #
    # db_write(sju_db,
    #          ais_data_accumulated_500_sql_drop,
    #          ais_data_accumulated_500_sql_create,
    #          ais_data_accumulated_500_sql_sequence,
    #          ais_data_accumulated_500_sql_primary,
    #          ais_data_accumulated_500_sql_grant,
    #          ais_data_accumulated_500_columns,
    #          vessel_500_orbcomn_df,
    #          'ais_data_accumulated_500')

    tug_static_area = pd.read_sql(tug_static_area_sql_select, sju_db)
    tug_static_area_df = tug_static_area[tug_static_area["TYPE"] == 'arrival_route']
    tug_static_area_df.reset_index(inplace=True, drop=True)

    op_routing_df = calculate_routes(PNIT_anchorage, tug_static_area_df, ais_data_accumulated, tos_vessel,
                                     tos_plan_berth_df)
    for idx in op_routing_df.index:
        vsl_nm = op_routing_df.loc[idx, "TOS_VSL_NM"]
        sql = f"SELECT * FROM VESSEL_HEADING_TO_BUSAN_TRACKING where TOS_VSL_NM = '{vsl_nm}'"
        df = pd.read_sql(sql, sju_db)
        try:
            series = df.sort_values(by='TIMESTAMP').iloc[0, :]
            LONGITUDE = series['LONGITUDE']
            LATITUDE = series['LATITUDE']
        except:
            LONGITUDE = 0
            LATITUDE = 0
        op_routing_df.loc[idx, "DEPARTING_LONGITUDE"] = LONGITUDE
        op_routing_df.loc[idx, "DEPARTING_LATITUDE"] = LATITUDE

    vssl_optimal_routing_df = op_routing_df[vssl_optimal_routing_columns]
    db_write(sju_db,
             vssl_optimal_routing_sql_drop,
             vssl_optimal_routing_sql_create,
             vssl_optimal_routing_sql_sequence,
             vssl_optimal_routing_sql_primary,
             vssl_optimal_routing_sql_grant,
             vssl_optimal_routing_columns,
             vssl_optimal_routing_df,
             'vssl_optimal_routing')

    original_tos_plan_berth_a_df = option_A_table(tos_plan_berth_df, tos_vessel)
    original_tos_plan_berth_a_df = original_tos_plan_berth_a_df[original_tos_plan_berth_a_columns]
    db_write(sju_db,
             original_tos_plan_berth_a_sql_drop,
             original_tos_plan_berth_a_sql_create,
             original_tos_plan_berth_a_sql_sequence,
             original_tos_plan_berth_a_sql_primary,
             original_tos_plan_berth_a_sql_grant,
             original_tos_plan_berth_a_columns,
             original_tos_plan_berth_a_df,
             'original_tos_plan_berth_a')

    b_final = option_B_table(tos_plan_berth_df, tos_vessel)
    terminal_optimal_plan_berth_b_df = b_final[terminal_optimal_plan_berth_b_columns]
    db_write(sju_db,
             terminal_optimal_plan_berth_b_sql_drop,
             terminal_optimal_plan_berth_b_sql_create,
             terminal_optimal_plan_berth_b_sql_sequence,
             terminal_optimal_plan_berth_b_sql_primary,
             terminal_optimal_plan_berth_b_sql_grant,
             terminal_optimal_plan_berth_b_columns,
             terminal_optimal_plan_berth_b_df,
             'terminal_optimal_plan_berth_b')

    mutual_optimal_plan_berth_c_df = option_C_table(pisces_url, tos_plan_berth_df, tos_vessel, ais_data_accumulated)
    mutual_optimal_plan_berth_c_df = mutual_optimal_plan_berth_c_df[mutual_optimal_plan_berth_c_columns]
    db_write(sju_db,
             mutual_optimal_plan_berth_c_sql_drop,
             mutual_optimal_plan_berth_c_sql_create,
             mutual_optimal_plan_berth_c_sql_sequence,
             mutual_optimal_plan_berth_c_sql_primary,
             mutual_optimal_plan_berth_c_sql_grant,
             mutual_optimal_plan_berth_c_columns,
             mutual_optimal_plan_berth_c_df,
             'mutual_optimal_plan_berth_c')

    tug_allocation_df = tug_allocation(tuglist, mutual_optimal_plan_berth_c_df, tos_vessel)
    tug_allocation_df = tug_allocation_df[tug_allocation_columns]
    db_write(sju_db,
             tug_allocation_sql_drop,
             tug_allocation_sql_create,
             tug_allocation_sql_sequence,
             tug_allocation_sql_primary,
             tug_allocation_sql_grant,
             tug_allocation_columns,
             tug_allocation_df,
             'tug_allocation')

    vssl_optimal_routing_df = pd.read_sql(vssl_optimal_routing_sql_select, sju_db)
    vessel_voyage_report7days_df = pd.read_sql(
        vessel_voyage_report_7days_sql_select, sju_db)
    option_a_df = pd.read_sql(option_a_select, sju_db)
    option_b_df = pd.read_sql(option_b_select, sju_db)
    option_c_df = pd.read_sql(option_c_select, sju_db)
    tos_plan_berth_atb_df = pd.read_sql(tos_plan_berth_atb_sql_select, tsb_db)
    tos_plan_berth_atd_df = pd.read_sql(tos_plan_berth_atd_sql_select, tsb_db)
    for idx in tos_plan_berth_atd_df.index:
        VSL_CD = tos_plan_berth_atd_df.loc[idx, "VSL_CD"]

        VSL_NM = tos_vessel.query(f"VSL_CD == '{VSL_CD}'")["VSL_NM"].values[0]
        LOA = tos_vessel.query(f"VSL_CD == '{VSL_CD}'")["LOA"].values[0]

        tos_plan_berth_atd_df.loc[idx, "VSL_NM"] = VSL_NM
        tos_plan_berth_atd_df.loc[idx, "LOA"] = LOA

    arrival_monitoring, on_vessel_df = arrival_monitoring_table(tos_vessel,
                                                                tos_plan_berth_atb_df, option_b_df,
                                                                option_c_df,
                                                                vessel_voyage_report7days_df, vssl_optimal_routing_df,
                                                                option_a_df,
                                                                ais_data_accumulated)
    db_write(sju_db,
             arrival_monitoring_sql_drop,
             arrival_monitoring_sql_create,
             None,
             None,
             arrival_monitoring_sql_grant,
             arrival_monitoring_columns,
             arrival_monitoring,
             'arrival_monitoring')

    departure_monitoring = departure_monitoring_table(on_vessel_df, pisces_url, tos_plan_berth_atd_df)
    departure_monitoring = departure_monitoring[departure_monitoring_columns]
    db_write(sju_db,
             departure_monitoring_sql_drop,
             departure_monitoring_sql_create,
             None,
             None,
             departure_monitoring_sql_grant,
             departure_monitoring_columns,
             departure_monitoring,
             'departure_monitoring')
    return no_mmsi_list

if __name__ == "__main__":
    cnt = 0
    error_cnt = 0
    try:
        location = sys.argv[1]
    except:
        location = "outside"

    while True:
        # try:
        os.system('cls')
        no_mmsi_list = main(location)
        cnt += 1
        print("Finished")
        print(f"현재까지 돌아간 횟수 : {cnt} (5분에 1번 돌아감) ")
        print(no_mmsi_list)
        time.sleep(300)
        # except:
        #     error_cnt += 1
        #     print(f"오류 발생 {error_cnt}회 째")
        #     time.sleep(60)
