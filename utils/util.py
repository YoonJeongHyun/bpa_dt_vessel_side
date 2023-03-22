import math
from selenium import webdriver
# from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
import time
import requests
import json
import datetime
import pandas as pd
from datetime import timedelta


# 선형적인 방법
def fuel_estimation(teu, T, S):
    T = T / 24
    if teu <= 1500:
        first_co_eff = 0.0072
        sec_co_eff = 10.8592
        estimated_fuel_consumption = (0.0072 * T * math.pow(S, 3)) + (10.8592 *
                                                                      T)

    elif teu <= 3000:
        first_co_eff = (teu - 1500) * 0.00000096 + 0.0072
        sec_co_dff = (teu - 1500) * 0.00312713 + 10.8592
        estimated_fuel_consumption = (first_co_eff * T *
                                      math.pow(S, 3)) + (sec_co_dff * T)

    elif teu <= 4500:
        first_co_eff = (teu - 3000) * 0.00000096 + 0.0072
        sec_co_dff = (teu - 3000) * 0.00312713 + 10.8592
        estimated_fuel_consumption = (first_co_eff * T *
                                      math.pow(S, 3)) + (sec_co_dff * T)

    elif teu <= 5750:
        first_co_eff = (teu - 4500) * 0.00000076 + 0.0101
        sec_co_dff = (teu - 4500) * 0.00467896 + 20.2406
        estimated_fuel_consumption = (first_co_eff * T *
                                      math.pow(S, 3)) + (sec_co_dff * T)

    elif teu <= 7000:
        first_co_eff = (teu - 5750) * 0.00000076 + 0.0101
        sec_co_dff = (teu - 5750) * 0.00467896 + 20.2406
        estimated_fuel_consumption = (first_co_eff * T *
                                      math.pow(S, 3)) + (sec_co_dff * T)

    elif teu <= 8500:
        first_co_eff = (teu - 7000) * 0.0000007 + 0.012
        sec_co_dff = (teu - 7000) * 0.0004734666 + 31.9380
        estimated_fuel_consumption = (first_co_eff * T *
                                      math.pow(S, 3)) + (sec_co_dff * T)

    elif teu <= 10000:
        first_co_eff = (teu - 8500) * 0.0000007 + 0.012
        sec_co_dff = (teu - 8500) * 0.0004734666 + 31.9380
        estimated_fuel_consumption = (first_co_eff * T *
                                      math.pow(S, 3)) + (sec_co_dff * T)

    elif teu <= 11750:
        first_co_eff = (teu - 10000) * 0.000001114 + 0.0141
        sec_co_dff = (teu - 10000) * 0.000027171428 + 32.1584
        estimated_fuel_consumption = (first_co_eff * T *
                                      math.pow(S, 3)) + (sec_co_dff * T)

    elif teu <= 13500:
        first_co_eff = (teu - 11750) * 0.000001114 + 0.0141
        sec_co_dff = (teu - 11750) * 0.000027171428 + 32.1584
        estimated_fuel_consumption = (first_co_eff * T *
                                      math.pow(S, 3)) + (sec_co_dff * T)

    else:
        first_co_eff = 0.0180
        sec_co_dff = 32.2535
        estimated_fuel_consumption = (first_co_eff * T *
                                      math.pow(S, 3)) + (sec_co_dff * T)

    return estimated_fuel_consumption

def upper(x):
    return str(x).upper()


def crawling_mmsi_new(unfound_vessel_list):
    from selenium.webdriver.common.keys import Keys

    vsl_name_list = unfound_vessel_list
    vsl_mmsi_dict = {"VSL_NM": [], "mmsi_no": [], "IMO": [], "CAPACITY" : []}

    driver = webdriver.Chrome("chromedriver")


    for vsl_name in vsl_name_list:
        driver.get('https://www.google.com/')
        time.sleep(1)
        input_tag = driver.find_element(By.CSS_SELECTOR, 'body > div.L3eUgb > div.o3j99.ikrT4e.om7nvf > form > div:nth-child(1) > div.A8SBwf > div.RNNXgb > div > div.a4bIc > input')
        input_tag.clear()
        input_tag.send_keys(vsl_name +" marine traffic")
        time.sleep(1)
        input_tag.send_keys(Keys.ENTER)
        time.sleep(1)

        search_list = driver.find_elements(By.CSS_SELECTOR, '#rso > div')
        for i in search_list:
            try:
                link = i.find_element(By.CSS_SELECTOR, 'div > div > div > div > a').get_attribute('href')
                if 'marinetraffic.com' in link:
                    txt = i.text.split('\n')
                    for i in txt:
                        if "TEU" in i:
                            capacity = int(i.split("is")[-1].strip().split(' ')[0])
                        elif "MMSI" in i:
                            MMSI = int(i.split("MMSI: ")[-1])
                        elif "IMO" in i:
                            IMO = int(i.split("IMO: ")[-1])

                    vsl_mmsi_dict['VSL_NM'].append(vsl_name)
                    vsl_mmsi_dict['mmsi_no'].append(MMSI)
                    vsl_mmsi_dict['IMO'].append(IMO)
                    vsl_mmsi_dict['CAPACITY'].append(capacity)

                break
            except:
                vsl_mmsi_dict['VSL_NM'].append(vsl_name)
                vsl_mmsi_dict['mmsi_no'].append(0)
                vsl_mmsi_dict['IMO'].append(0)
                vsl_mmsi_dict['CAPACITY'].append(0)

    driver.close()
    return vsl_mmsi_dict

# def orbcomn_fleet_delete(username='sewonkim', password='sejong@)@!12'):
#     try:
#         fleet_search = f'https://api.commtrace.com/ais/v1.1/fleet?username={username}&password={password}'
#         api_call_response = requests.get(fleet_search, verify=False)
#         fleet_list = json.loads(api_call_response.text)
#         fleet_list = fleet_list["fleet_info_list"]
#         fleet_id_list = []
#         for i in range(len(fleet_list)):
#             fleet_id = fleet_list[i]["fleet_id"]
#             fleet_id_list.append(fleet_id)
#
#         # delete the fleets
#         for fleet_id in fleet_id_list:
#             delete_url = f'https://api.commtrace.com/ais/v1.1/fleet/{fleet_id}?username={username}&password={password}'
#             requests.delete(delete_url, verify=False)
#     except:
#         print("fleet delete error occurs")


def orbcomn_fleet_create(username='sewonkim', password='sejong@)@!12'):
    fleet_name = 'fleet001'
    desc = 'fleet001_'

    fleet_register_url = f'https://api.commtrace.com/ais/v1.1/fleet?name={fleet_name}&description={desc}&username={username}&password={password}'

    api_call_response = requests.post(fleet_register_url, verify=False)
    root = json.loads(api_call_response.text)
    fleet_id = root["fleet_id"]
    return fleet_id


def orbcomn_fleet_post_vessels(observing_mmsi_list, fleet_id, username='sewonkim', password='sejong@)@!12'):
    mmsi_list = observing_mmsi_list

    fleet_mmsi_list = []
    total_page = orbcomn_fleet_max_page(fleet_id)
    for page in range(total_page):
        time.sleep(1)
        test_api_url = f"https://api.commtrace.com/ais/v1.1/fleet/{fleet_id}?username={username}&password={password}&page={page + 1}"
        api_call_response = requests.get(test_api_url, verify=False)
        root = json.loads(api_call_response.text)
        for i in range(len(root["vessel_info_list"])):
            mmsi = root["vessel_info_list"][i]["mmsi"]
            fleet_mmsi_list.append(mmsi)

    omit_mmsi_list = list(set(fleet_mmsi_list) - set(mmsi_list))
    new_mmsi_list = list(set(mmsi_list) - set(fleet_mmsi_list))
    if len(new_mmsi_list) != 0:
        mmsi_str = ""
        cnt = 0
        number_of_vessels = len(new_mmsi_list)
        for mmsi in new_mmsi_list:
            mmsi_str += f"&mmsiList={mmsi}"
            cnt += 1
            if cnt % 100 == 0:
                test_api_url = f"https://api.commtrace.com/ais/v1.1/fleet/{fleet_id}?username={username}&password={password}{mmsi_str}"
                api_call_response = requests.post(test_api_url, verify=False)
                mmsi_str = ""
                print("The number of vessels : ", cnt)
                #         cnt = 0
                time.sleep(3)
            if cnt == number_of_vessels:
                test_api_url = f"https://api.commtrace.com/ais/v1.1/fleet/{fleet_id}?username={username}&password={password}{mmsi_str}"
                api_call_response = requests.post(test_api_url, verify=False)
                print("The number of vessels : ", cnt)
                print("AIS DATA updated")
    return omit_mmsi_list


def orbcomn_fleet_max_page(fleet_id, username='sewonkim', password='sejong@)@!12'):
    test_api_url = f"https://api.commtrace.com/ais/v1.1/fleet/{fleet_id}?username={username}&password={password}&page=1"
    api_call_response = requests.get(test_api_url, verify=False)
    root = json.loads(api_call_response.text)
    total_page = root["page_info"]["total_page"]

    return total_page


def calculate_terminal_working_time(initplan, url):
    terminal_working_hour_list = []
    for idx in initplan.index:
        terminal_working_hour_dict = {
            "vvd": '',
            "etb": '',
            "etd": '',
            "dqty": '',
            "lqty": '',
            "sqty": '',
            "dqtyTeu": '',
            "lqtyTeu": '',
            "sqtyTeu": '',
            "berthNo": '',
            "sbitt": '',
            "ebitt": '',
            "loa": ''
        }
        VOYAGE = initplan.loc[idx, "VOYAGE"]
        BERTH_NO = initplan.loc[idx, "BERTH_NO"]
        ETB = initplan.loc[idx, "ETB"]
        ETD = initplan.loc[idx, "ETD"]
        DIS_QTY = initplan.loc[idx, "DIS_QTY"]
        LOAD_QTY = initplan.loc[idx, "LOAD_QTY"]
        FROM_BITT = initplan.loc[idx, "FROM_BITT"]
        TO_BITT = initplan.loc[idx, "TO_BITT"]
        LOA = initplan.loc[idx, "LOA"]

        ETB = str(ETB)
        ETD = str(ETD)
        DIS_QTY = int(DIS_QTY)
        LOAD_QTY = int(LOAD_QTY)
        FROM_BITT = int(FROM_BITT)
        TO_BITT = int(TO_BITT)
        LOA = int(LOA)

        terminal_working_hour_dict['vvd'] = VOYAGE
        terminal_working_hour_dict['etb'] = ETB
        terminal_working_hour_dict['etd'] = ETD
        terminal_working_hour_dict['dqty'] = DIS_QTY
        terminal_working_hour_dict['lqty'] = LOAD_QTY
        terminal_working_hour_dict['sqty'] = 0
        terminal_working_hour_dict['dqtyTeu'] = 0
        terminal_working_hour_dict['lqtyTeu'] = 0
        terminal_working_hour_dict['sqtyTeu'] = 0
        terminal_working_hour_dict['berthNo'] = BERTH_NO
        terminal_working_hour_dict['sbitt'] = FROM_BITT
        terminal_working_hour_dict['ebitt'] = TO_BITT
        terminal_working_hour_dict['loa'] = LOA

        terminal_working_hour_list.append(terminal_working_hour_dict)
    data = json.dumps(terminal_working_hour_list)
    api_call_headers = {
        "Content-Type": 'application/json',
        'accept': 'application/json',
    }
    api_call_response = requests.post(url,
                                      data=data,
                                      headers=api_call_headers,
                                      verify=False)
    result = json.loads(api_call_response.text)
    dict_ = {"VOYAGE": [], "work": []}
    for i in result:
        voyage = i["vs"]["vvd"]
        #     preProcessTime = i["preProcessTime"]
        totWorkTime = i["totWorkTime"]
        #     totWorkTime = totWorkTime + preProcessTime
        dict_["VOYAGE"].append(voyage)
        dict_["work"].append(totWorkTime)
    working_time_df = pd.DataFrame(dict_)
    working_time_df["work"] = working_time_df["work"].apply(
        lambda x: timedelta(seconds=x))

    return working_time_df