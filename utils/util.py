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


def fuel_estimation(teu, T, S):
    if teu <= 1500:
        first_co_eff = 0.0072
        sec_co_eff = 10.8592
        estimated_fuel_consumption = (first_co_eff * T * math.pow(S, 3)) + (sec_co_eff * T)

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


def crawling_mmsi(vsl_name_list):
    # 웹 드라이버 실행
    vsl_mmsi_dict = {"VSL_NM": [], "mmsi_no": [], "IMO": []}
    if len(vsl_name_list) == 0:
        return vsl_mmsi_dict

    driver = webdriver.Chrome("chromedriver")
    driver.get('https://www.vesselfinder.com/vessels')

    for vsl_name in vsl_name_list:
        input_tag = driver.find_element(By.CSS_SELECTOR, '#advsearch-name')

        input_tag.clear()
        input_tag.send_keys(vsl_name)
        time.sleep(1)

        submit_btn = driver.find_element(
            By.CSS_SELECTOR,
            'body > div > div > main > div > form > div > button')
        submit_btn.click()
        if len(driver.find_elements(By.CSS_SELECTOR,
                                    'body > div > div > main > div > section > table > tbody > tr')) == 1:
            ship_link = driver.find_element(
                By.CSS_SELECTOR,
                'body > div > div > main > div > section > table > tbody > tr > td > a'
            )
            ship_link = ship_link.get_attribute('href')
            driver.get(ship_link)
            time.sleep(1)

            detail = driver.find_element(
                By.CSS_SELECTOR,
                'body > div.body-wrapper > div > main > div > div.column.ship-section > p.text1')
            imo = detail.text.split("IMO: ")[1].split(",")[0]
            mmsi = detail.text.split("MMSI ")[1].split(")")[0]

            vsl_mmsi_dict["VSL_NM"].append(vsl_name)
            vsl_mmsi_dict["IMO"].append(imo)
            vsl_mmsi_dict["mmsi_no"].append(mmsi)
        else:
            ship_link = driver.find_element(
                By.CSS_SELECTOR,
                'body > div > div > main > div > section > table > tbody > tr > td > a'
            )
            ship_link = ship_link.get_attribute('href')
            driver.get(ship_link)
            time.sleep(1)

            detail = driver.find_element(
                By.CSS_SELECTOR,
                'body > div.body-wrapper > div > main > div > div.column.ship-section > p.text1')
            if not detail.text.split("is")[1].split("built")[0].strip() == "a Container Ship":
                continue
            else:
                imo = detail.text.split("IMO: ")[1].split(",")[0]
                mmsi = detail.text.split("MMSI ")[1].split(")")[0]

                vsl_mmsi_dict["VSL_NM"].append(vsl_name)
                vsl_mmsi_dict["IMO"].append(imo)
                vsl_mmsi_dict["mmsi_no"].append(mmsi)
        driver.get('https://www.vesselfinder.com/vessels')
        time.sleep(1)

    driver.close()
    return vsl_mmsi_dict


def capacity_crawling(tos_vessel, no_capa_vessel):
    no_capa_dict = {"VSL_NM": [], "CAPACITY": []}
    if len(no_capa_vessel) == 0:
        return no_capa_dict

    new_capa_vessel = []

    for vsl in no_capa_vessel:
        try:
            teu = tos_vessel.query(f"VSL_NM == '{vsl}'")["CAPACITY"].values[0]
            teu = int(teu)
            if teu != 0:
                no_capa_dict['VSL_NM'].append(vsl)
                no_capa_dict["CAPACITY"].append(teu)
            else:
                new_capa_vessel.append(vsl)
        except:
            new_capa_vessel.append(vsl)

    driver = webdriver.Chrome("chromedriver")

    for vsl in new_capa_vessel:
        query_str = ''
        name = vsl.split(" ")
        for idx, element in enumerate(name):
            query_str += element
            if idx != len(name) - 1:
                query_str += "+"

        driver.get(f'https://www.vesseltracking.net/ships?search={query_str}')
        time.sleep(1)
        teu = None
        try:
            teu = driver.find_element(
                By.CSS_SELECTOR, '#w0 > table > tbody > tr > td:nth-child(5)')
            teu = teu.text
            teu = int(teu)
            no_capa_dict["VSL_NM"].append(vsl)
            no_capa_dict["CAPACITY"].append(teu)
        except:
            no_capa_dict["VSL_NM"].append(vsl)
            no_capa_dict["CAPACITY"].append(0)

    driver.close()
    return no_capa_dict


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