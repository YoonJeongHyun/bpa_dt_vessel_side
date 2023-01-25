import pandas as pd
from utils.util import *
import numpy as np
import requests
import json
import datetime
from datetime import timedelta
from haversine import haversine
from pyproj import Geod
from shapely.geometry import Point, Polygon


def search_for_vessels(tos_plan_berth_df, tos_vessel):
    for idx in tos_plan_berth_df.index:
        vsl_cd = tos_plan_berth_df.loc[idx, "VSL_CD"]
        tos_plan_berth_df.loc[idx, "VSL_NM"] = tos_vessel.query(f"VSL_CD == '{vsl_cd}'")["VSL_NM"].values[0]

    tos_plan_berth_df.sort_values(by="ETA", inplace=True)
    tos_plan_berth_df.reset_index(drop=True, inplace=True)
    vsl_name_dict = dict.fromkeys(tos_plan_berth_df["VSL_NM"].values)
    vsl_name_list = list(vsl_name_dict)
    mmsi_vessel_df = pd.read_csv("mmsi_vessel_list.csv")
    unfound_vessel_list = []
    for name in vsl_name_list:
        if name not in mmsi_vessel_df["VSL_NM"].values:
            #         if name != 'MSC YUVIKA':
            if name == np.nan:
                unfound_vessel_list.append(name)
            unfound_vessel_list.append(name)

    if len(unfound_vessel_list) != 0:
        vsl_mmsi_dict_new = crawling_mmsi_new(unfound_vessel_list)
        dfnew = pd.DataFrame(vsl_mmsi_dict_new)
        dfnew.to_csv('mmsi_vessel_list.csv',
                     header=False,
                     index=False,
                     encoding="utf-8 sig", mode="a")
    mmsi_vessel_df = pd.read_csv('mmsi_vessel_list.csv')
    mmsi_vessel_df.dropna(inplace=True)

    observing_mmsi_list = []
    no_mmsi_list = []
    for vsl_nm in vsl_name_list:
        mmsi_vessel_df["mmsi_no"] = mmsi_vessel_df["mmsi_no"].apply(lambda x: int(x))
        mmsi = mmsi_vessel_df.query(f'VSL_NM == "{vsl_nm}"')["mmsi_no"].values[0]
        if mmsi == 0:
            no_mmsi_list.append(vsl_nm)
            continue
        if np.isnan(mmsi):
            no_mmsi_list.append(vsl_nm)
            continue
        if len(str(mmsi)) > 9:
            mmsi = str(mmsi)[:-2]
        observing_mmsi_list.append(mmsi)

    return observing_mmsi_list, mmsi_vessel_df, no_mmsi_list


def ais_monitoring(observing_mmsi_list, mode="ais", mmsi_vessel_df=None, tos_plan_berth_df=None, username='sewonkim',
                   password='sejong@)@!12'):
    # POST
    if mode == "ais":
        # fleet_id = orbcomn_fleet_create()
        # fleet_id = "sewonkim8390"
        fleet_id = "sewonkim8393"
        omit_mmsi_list = orbcomn_fleet_post_vessels(observing_mmsi_list, fleet_id)
        total_page = orbcomn_fleet_max_page(fleet_id)

        data_dict = {
            "vessel_name": [],
            "tos_vessel_name": [],
            "mmsi": [],
            "imo": [],
            "eta": [],
            "destination": [],
            "navigation_info": [],
            "longitude": [],
            "latitude": [],
            "heading": [],
            "speed": [],
            "max_draught": [],
            "time_position_data_received": [],
            "time_voyage_data_received": [],
            "CAPACITY": [],
            "tos_ETA": [],
            "tos_ETD": [],
            "VOYAGE": [],
            "DIS_QTY": [],
            "LOAD_QTY": []
        }

        for page in range(total_page):
            time.sleep(1)
            test_api_url = f"https://api.commtrace.com/ais/v1.1/fleet/{fleet_id}?username={username}&password={password}&page={page + 1} "
            api_call_response = requests.get(test_api_url, verify=False)
            root = json.loads(api_call_response.text)
            for i in range(len(root["vessel_info_list"])):
                mmsi = root["vessel_info_list"][i]["mmsi"]
                if mmsi == 636018635:
                    continue
                if mmsi in omit_mmsi_list:
                    continue

                destination = root["vessel_info_list"][i]["voyage_info"]["destination"]
                if destination is not None:
                    destination = destination.replace(">>", ">")
                    destination = destination.replace("->", ">")
                    destination = destination.replace("'", "")
                    destination = destination.replace('"', '')
                    destination = destination.replace('', '')
                    if "-" in destination:
                        destination = destination.split('-')[1].strip(" ")

                    elif ">" in destination:
                        destination = destination.split('>')[1].strip(" ")
                else:
                    destination = "None"
                data_dict["mmsi"].append(mmsi)
                data_dict["imo"].append(root["vessel_info_list"][i]["imo"])
                data_dict["eta"].append(
                    root["vessel_info_list"][i]["voyage_info"]["eta"])
                vessel_name = root["vessel_info_list"][i]["vessel_name"]
                data_dict["vessel_name"].append(vessel_name)
                destination = destination.upper()
                data_dict["destination"].append(destination)
                try:
                    data_dict["navigation_info"].append(
                        root["vessel_info_list"][i]["voyage_info"]["navigation_info"]
                        ['description'])
                except:
                    data_dict["navigation_info"].append(None)
                data_dict["longitude"].append(
                    root["vessel_info_list"][i]["position_info"]["lon"])
                data_dict["latitude"].append(
                    root["vessel_info_list"][i]["position_info"]["lat"])
                data_dict["heading"].append(
                    root["vessel_info_list"][i]["position_info"]["true_heading"])
                data_dict["speed"].append(
                    root["vessel_info_list"][i]["position_info"]["speed"])
                data_dict["time_position_data_received"].append(
                    root["vessel_info_list"][i]["position_info"]["received"])
                data_dict["time_voyage_data_received"].append(
                    root["vessel_info_list"][i]["voyage_info"]["received"])
                max_draught = root["vessel_info_list"][i]["voyage_info"]["max_draught"]
                data_dict["max_draught"].append(max_draught)

                try:
                    try:
                        mmsi_df = mmsi_vessel_df.query(f'mmsi_no == {mmsi}')
                        capacity = mmsi_df["CAPACITY"].values[0]
                        tos_vessel_name = mmsi_df["VSL_NM"].values[0]
                        tos_df = tos_plan_berth_df.query(
                            f"VSL_NM == '{tos_vessel_name}'")
                        if len(tos_df) == 0:
                            tos_vessel_name = mmsi_df["VSL_NM"].values[1]
                            tos_df = tos_plan_berth_df.query(
                                f"VSL_NM == '{tos_vessel_name}'")

                        tos_ETA = tos_df["ETA"].values[0]
                        tos_ETD = tos_df["ETD"].values[0]
                        if pd.isnull(tos_ETA):
                            tos_ETA = datetime.datetime(year=1970, month=1, day=1)
                        if pd.isnull(tos_ETD):
                            tos_ETD = datetime.datetime(year=1970, month=1, day=1)
                        VOYAGE = tos_df["VOYAGE"].values[0]
                        DIS_QTY = tos_df["DIS_QTY"].values[0]
                        LOAD_QTY = tos_df["LOAD_QTY"].values[0]

                    except:
                        mmsi_df = mmsi_vessel_df.query(f'VSL_NM == "{vessel_name}"')
                        capacity = mmsi_df["CAPACITY"].values[0]
                        tos_vessel_name = mmsi_df["VSL_NM"].values[0]
                        tos_df = tos_plan_berth_df.query(
                            f"VSL_NM == '{tos_vessel_name}'")
                        if len(tos_df) == 0:
                            tos_vessel_name = mmsi_df["VSL_NM"].values[1]
                            tos_df = tos_plan_berth_df.query(
                                f"VSL_NM == '{tos_vessel_name}'")

                        tos_ETA = tos_df["ETA"].values[0]
                        tos_ETD = tos_df["ETD"].values[0]
                        if pd.isnull(tos_ETA):
                            tos_ETA = datetime.datetime(year=1970, month=1, day=1)
                        if pd.isnull(tos_ETD):
                            tos_ETD = datetime.datetime(year=1970, month=1, day=1)
                        VOYAGE = tos_df["VOYAGE"].values[0]
                        DIS_QTY = tos_df["DIS_QTY"].values[0]
                        LOAD_QTY = tos_df["LOAD_QTY"].values[0]
                except:
                    capacity = 0
                    tos_vessel_name = ""
                    tos_ETA = ''
                    tos_ETD = ''
                    VOYAGE = ""
                    DIS_QTY = ""
                    LOAD_QTY = ""

                data_dict["CAPACITY"].append(capacity)
                data_dict["tos_vessel_name"].append(tos_vessel_name)
                data_dict["tos_ETA"].append(tos_ETA)
                data_dict["tos_ETD"].append(tos_ETD)
                data_dict["VOYAGE"].append(VOYAGE)
                data_dict["DIS_QTY"].append(DIS_QTY)
                data_dict["LOAD_QTY"].append(LOAD_QTY)

    # elif mode == 'ais_500':
    #     # fleet_id = orbcomn_fleet_create()
    #     fleet_id = 'sewonkim8391'
    #     print("=" * 100)
    #     print("400 vessels monitored")
    #     # orbcomn_fleet_post_vessels(observing_mmsi_list, fleet_id)
    #     total_page = orbcomn_fleet_max_page(fleet_id)
    #
    #     data_dict = {
    #         "vessel_name": [],
    #         "excel_vessel_name": [],
    #         "mmsi": [],
    #         "imo": [],
    #         "eta": [],
    #         "destination": [],
    #         "navigation_info": [],
    #         "longitude": [],
    #         "latitude": [],
    #         "heading": [],
    #         "speed": [],
    #         "max_draught": [],
    #         "time_position_data_received": [],
    #         "time_voyage_data_received": []
    #     }
    #     for page in range(total_page):
    #         time.sleep(1)
    #         test_api_url = f"https://api.commtrace.com/ais/v1.1/fleet/{fleet_id}?username={username}&password={password}&page={page + 1}"
    #         api_call_response = requests.get(test_api_url, verify=False)
    #         root = json.loads(api_call_response.text)
    #         for i in range(len(root["vessel_info_list"])):
    #             mmsi = root["vessel_info_list"][i]["mmsi"]
    #             data_dict["mmsi"].append(mmsi)
    #             data_dict["imo"].append(root["vessel_info_list"][i]["imo"])
    #             data_dict["eta"].append(
    #                 root["vessel_info_list"][i]["voyage_info"]["eta"])
    #             data_dict["vessel_name"].append(
    #                 root["vessel_info_list"][i]["vessel_name"])
    #             data_dict["destination"].append(
    #                 root["vessel_info_list"][i]["voyage_info"]["destination"])
    #             try:
    #                 data_dict["navigation_info"].append(
    #                     root["vessel_info_list"][i]["voyage_info"]["navigation_info"]
    #                     ['description'])
    #             except:
    #                 data_dict["navigation_info"].append(None)
    #             data_dict["longitude"].append(
    #                 root["vessel_info_list"][i]["position_info"]["lon"])
    #             data_dict["latitude"].append(
    #                 root["vessel_info_list"][i]["position_info"]["lat"])
    #             data_dict["heading"].append(
    #                 root["vessel_info_list"][i]["position_info"]["true_heading"])
    #             data_dict["speed"].append(
    #                 root["vessel_info_list"][i]["position_info"]["speed"])
    #             data_dict["time_position_data_received"].append(
    #                 root["vessel_info_list"][i]["position_info"]["received"])
    #             data_dict["time_voyage_data_received"].append(
    #                 root["vessel_info_list"][i]["voyage_info"]["received"])
    #             max_draught = root["vessel_info_list"][i]["voyage_info"]["max_draught"]
    #             data_dict["max_draught"].append(max_draught)
    #             try:
    #                 excel_vessel_name = mmsi_vessel_df.query(f'MMSI_NO == {mmsi}')['VSSL_ENG_NM'].values[0]
    #                 data_dict['excel_vessel_name'].append(excel_vessel_name)
    #             except:
    #                 data_dict['excel_vessel_name'].append("")

    return data_dict


def make_busan_arriving_vessels_df(data_dict, PNIT_anchorage):
    pnit_vessel_df = pd.DataFrame(data_dict)
    pnit_vessel_df.reset_index(inplace=True, drop=True)
    today = datetime.datetime.now()
    one_day = timedelta(days=1)
    # busan_arriving_vessels = pd.DataFrame(data_dict)
    busan_arriving_vessels = pnit_vessel_df
    busan_arriving_vessels["time_position_data_received"] = pd.to_datetime(
        busan_arriving_vessels["time_position_data_received"].apply(
            lambda x: x.replace("T", " ").replace("Z", "")))
    busan_arriving_vessels["time_voyage_data_received"] = pd.to_datetime(
        busan_arriving_vessels["time_voyage_data_received"].apply(
            lambda x: x.replace("T", " ").replace("Z", "")))
    time_difference = busan_arriving_vessels[
                          "time_position_data_received"] - busan_arriving_vessels[
                          "time_voyage_data_received"]
    busan_arriving_vessels["time_difference"] = time_difference
    busan_arriving_vessels["isnottoday"] = today - busan_arriving_vessels[
        "time_position_data_received"]
    busan_arriving_vessels["timestamp"] = today
    # big_gap_df = busan_arriving_vessels[
    #     busan_arriving_vessels["isnottoday"] > timedelta(days=6)]
    # busan_arriving_vessels.drop(big_gap_df.index, inplace=True)

    busan_arriving_vessels.reset_index(inplace=True, drop=True)
    busan_arriving_vessels["remained_distance"] = ''
    busan_arriving_vessels["predicted_ETA"] = ''
    busan_arriving_vessels["estimated_fuel_consumption"] = 0

    for idx, val in enumerate(busan_arriving_vessels.values):
        latitude = busan_arriving_vessels.loc[idx]["latitude"]
        longitude = busan_arriving_vessels.loc[idx]["longitude"]

        current_coord = (latitude, longitude)
        remain_distance_km = haversine(current_coord, PNIT_anchorage, unit="km")
        busan_arriving_vessels.loc[idx, 'remained_distance'] = remain_distance_km

    # inaccurate_time_df["estimated_fuel_consumption"] = 0

    for idx, teu in enumerate(busan_arriving_vessels["CAPACITY"].values):
        #     print(idx)
        mile = float(busan_arriving_vessels.loc[idx, "remained_distance"]) / 1.852
        S = busan_arriving_vessels.loc[idx, 'speed']
        T = mile / S
        try:
            teu = int(teu)
            estimated_fuel_consumption = fuel_estimation(teu, T, S)
            busan_arriving_vessels.loc[
                idx, "estimated_fuel_consumption"] = estimated_fuel_consumption
        except:
            busan_arriving_vessels.loc[
                idx, "estimated_fuel_consumption"] = 0

    cal_idx = busan_arriving_vessels.query(
        'navigation_info == "under way using engine"').index
    remained_distance = busan_arriving_vessels.loc[cal_idx, 'remained_distance']
    speed_series = busan_arriving_vessels.loc[cal_idx, "speed"] * 1.852
    time_position_data_received = busan_arriving_vessels.loc[
        cal_idx, 'time_position_data_received']
    for idx, remained_distance in zip(remained_distance.index,
                                      remained_distance.values):
        speed = speed_series[idx]
        if speed != 0.0:
            hour = int(remained_distance // speed)
            minute = int(
                (remained_distance / speed - remained_distance // speed) * 60)
            position_time = time_position_data_received[idx]
            predicted_eta = position_time + datetime.timedelta(hours=hour,
                                                               minutes=minute)
            busan_arriving_vessels.loc[idx, 'predicted_ETA'] = predicted_eta
        else:
            false = datetime.datetime.strptime("1970-01-01 11:59:59", "%Y-%m-%d %H:%M:%S")
            busan_arriving_vessels.loc[idx, 'predicted_ETA'] = false
    busan_arriving_vessels = busan_arriving_vessels.sort_values(by="tos_ETA")
    busan_arriving_vessels.reset_index(inplace=True, drop=True)

    for idx in busan_arriving_vessels.index:
        if np.isnan(busan_arriving_vessels.loc[idx, 'estimated_fuel_consumption']):
            busan_arriving_vessels.loc[idx, 'estimated_fuel_consumption'] = 0
        if np.isnan(busan_arriving_vessels.loc[idx, 'heading']):
            busan_arriving_vessels.loc[idx, 'heading'] = 0
        if np.isnan(busan_arriving_vessels.loc[idx, 'max_draught']):
            busan_arriving_vessels.loc[idx, 'max_draught'] = 0
    for idx in busan_arriving_vessels.query("predicted_ETA ==''").index:
        busan_arriving_vessels.loc[idx, 'predicted_ETA'] = datetime.datetime.strptime("1970-01-01 11:59:59",
                                                                                      "%Y-%m-%d %H:%M:%S")

    busan_arriving_vessels.rename(columns={
        "vessel_name": "VSL_NM",
        "mmsi": 'MMSI',
        'CAPACITY': "capacity",
        "tos_vessel_name": 'tos_VSL_NM'
    },
        inplace=True)
    busan_arriving_vessels.drop(columns=['imo', 'eta'], inplace=True)

    busan_arriving_vessels["s_id"] = 'seq_ais_acc.NEXTVAL'

    busan_arriving_vessels["timestamp"] = busan_arriving_vessels["timestamp"].apply(
        lambda x: pd.to_datetime(str(x)[:-7]))
    return busan_arriving_vessels


def tracking_vessels_monitoring(tracking_vsl_nm_list_df, ais_data_accumulated, previous_ais_data_accumulated):
    duplicated_check_list = tracking_vsl_nm_list_df["TOS_VSL_NM"].values

    tracking_vsl_nm_dict = {"TOS_VSL_NM": [], "DESTINATION": []}

    for idx in ais_data_accumulated.index:
        vsl_nm_current_ais = ais_data_accumulated.loc[idx, "TOS_VSL_NM"]
        destination_current_ais = ais_data_accumulated.loc[idx, "DESTINATION"]
        if destination_current_ais == None:
            continue
        # 과거 데이터 중 없었던 선박이었는데 부산항을 향해 오는 선박 체크
        if vsl_nm_current_ais not in previous_ais_data_accumulated[
            "TOS_VSL_NM"].values:
            if ('KR' in destination_current_ais) or (
                    "BUS" in destination_current_ais) or (
                    "PUS" in destination_current_ais) or (
                    "BNP" in destination_current_ais):
                tracking_vsl_nm_dict['TOS_VSL_NM'].append(vsl_nm_current_ais)
                tracking_vsl_nm_dict['DESTINATION'].append(destination_current_ais)
                continue

        for idx2 in previous_ais_data_accumulated.index:
            vsl_nm_previous_ais = previous_ais_data_accumulated.loc[idx2,
                                                                    "TOS_VSL_NM"]
            destination_previous_ais = previous_ais_data_accumulated.loc[
                idx2, "DESTINATION"]
            # 전 선박과 이번 선박의 목적지가 다른 경우
            if not vsl_nm_current_ais in duplicated_check_list:

                if (vsl_nm_current_ais == vsl_nm_previous_ais) and (
                        destination_previous_ais != destination_current_ais):
                    # 현재 목적지가 부산일 때 tracking 시작

                    if ("BUS" in destination_current_ais) or (
                            "PUS" in destination_current_ais) or (
                            "BNP" in destination_current_ais):
                        tracking_vsl_nm_dict['TOS_VSL_NM'].append(
                            vsl_nm_current_ais)
                        tracking_vsl_nm_dict['DESTINATION'].append(
                            destination_current_ais)
    tracking_vsl_df = pd.DataFrame(tracking_vsl_nm_dict)
    return tracking_vsl_df


def check_vessels_in_boundary_of_pnit(ais_data_accumulated, sju_db):
    pnit_anchorage_envelop = '128.7671906822076,34.81844137658872,-8.169979588701725 129.0023032803867,' \
                             '34.85522468420153,9.345512809660946 128.9643016241988,35.06662140667149,' \
                             '5.605417239082692 128.7342482601284,35.06589002695234,' \
                             '5.494114199792744 128.7671906822076,34.81844137658872,-8.169979588701725'
    coord_list = pnit_anchorage_envelop.split(" ")
    coord_list2 = []
    for i in coord_list:
        temp = i.split(",")[:2]
        temp = list(map(float, temp))
        coord_list2.append(temp)
    poly = Polygon(coord_list2)

    vessels_anchoring_at_busan_list = []
    for idx in ais_data_accumulated.index:
        TOS_VSL_NM = ais_data_accumulated.loc[idx, "TOS_VSL_NM"]
        destination = ais_data_accumulated.loc[idx, "DESTINATION"]
        LONGITUDE = ais_data_accumulated.loc[idx, "LONGITUDE"]
        LATITUDE = ais_data_accumulated.loc[idx, "LATITUDE"]
        NAVIGATION_INFO = ais_data_accumulated.loc[idx, "NAVIGATION_INFO"]
        # pnit 앵커리지 근처에 위치가 찍혔을 때
        if Point(LONGITUDE, LATITUDE).within(poly):
            vessels_anchoring_at_busan_list.append(TOS_VSL_NM)
        elif NAVIGATION_INFO != 'under way using engine':
            if destination is not None:
                if ('KR' in destination) or ("BUS" in destination) or ("PUS" in destination) or ("BNP" in destination):
                    vessels_anchoring_at_busan_list.append(TOS_VSL_NM)

    dict_values = {
        'VSL_NM': [],
        'TOS_VSL_NM': [],
        'MMSI': [],
        'VOYAGE': [],
        'DESTINATION': [],
        'DEPARTING_LONGITUDE': [],
        'DEPARTING_LATITUDE': [],
        'MEAN_SPEED': [],
        'TIMESTAMP': [],
        'TOS_ETA': [],
        'TOS_ETD': [],
        'CAPACITY': [],
        'OVERALL_DISTANCE': [],
        'FINAL_ESTIMATED_FUEL_CONSUMPTION': [],
        'FINAL_CO2_EMISSION': [],
        "PREDICTED_ETA": []
    }
    for vsl_nm in vessels_anchoring_at_busan_list:
        stop_tracking_sql = f"SELECT * FROM vessel_heading_to_busan_tracking WHERE TOS_VSL_NM = '{vsl_nm}'"
        stop_tracking_df = pd.read_sql(stop_tracking_sql, sju_db)

        if len(stop_tracking_df) == 0:
            continue

        try:
            drop_idx = stop_tracking_df.query('SPEED < 2.0').index
            stop_tracking_df.drop(drop_idx, inplace=True)
            stop_tracking_df.reset_index(drop=True, inplace=True)
            MEAN_SPEED = stop_tracking_df["SPEED"].mean()
            CAPACITY = stop_tracking_df["CAPACITY"].values[0]
            print("=" * 100)
        except:
            print("")
            continue

        try:
            stop_tracking_df.sort_values(by='TIMESTAMP', inplace=True)
            first_value = stop_tracking_df.head(1)
            DEPARTING_LONGITUDE = first_value["LONGITUDE"].values[0]
            DEPARTING_LATITUDE = first_value["LATITUDE"].values[0]
            OVERALL_DISTANCE = first_value['REMAINED_DISTANCE'].values[0]
            VSL_NM = first_value['VSL_NM'].values[0]
            TOS_VSL_NM = first_value['TOS_VSL_NM'].values[0]
            MMSI = first_value['MMSI'].values[0]
            VOYAGE = first_value['VOYAGE'].values[0]
            DESTINATION = first_value['DESTINATION'].values[0]
            TIMESTAMP = first_value['TIMESTAMP'].values[0]
            TOS_ETA = first_value['TOS_ETA'].values[0]
            TOS_ETD = first_value['TOS_ETD'].values[0]
            PREDICTED_ETA = first_value['PREDICTED_ETA'].values[0]
            print("=" * 100)

        except:
            print("no vessel available")

        try:
            mile = float(OVERALL_DISTANCE) / 1.852
            T = mile / MEAN_SPEED
            estimated_fuel_consumption = fuel_estimation(CAPACITY, T, MEAN_SPEED)
            co2_emission = estimated_fuel_consumption * 3.11440
        except:
            print("no_fuel_consumption")
        try:
            dict_values['VSL_NM'].append(VSL_NM)
            dict_values['TOS_VSL_NM'].append(TOS_VSL_NM)
            dict_values['MMSI'].append(MMSI)
            dict_values['VOYAGE'].append(VOYAGE)
            dict_values['DESTINATION'].append(DESTINATION)
            dict_values['DEPARTING_LONGITUDE'].append(DEPARTING_LONGITUDE)
            dict_values['DEPARTING_LATITUDE'].append(DEPARTING_LATITUDE)
            dict_values['MEAN_SPEED'].append(MEAN_SPEED)
            dict_values['TIMESTAMP'].append(TIMESTAMP)
            dict_values['TOS_ETA'].append(TOS_ETA)
            dict_values['TOS_ETD'].append(TOS_ETD)
            dict_values['CAPACITY'].append(CAPACITY)
            dict_values['OVERALL_DISTANCE'].append(OVERALL_DISTANCE)
            dict_values['FINAL_ESTIMATED_FUEL_CONSUMPTION'].append(estimated_fuel_consumption)
            dict_values['FINAL_CO2_EMISSION'].append(co2_emission)
            dict_values['PREDICTED_ETA'].append(PREDICTED_ETA)
        except:
            print("dictionary error!")

    vessel_voyage_report = pd.DataFrame(dict_values)

    return vessels_anchoring_at_busan_list, vessel_voyage_report


def make_vessel_500_orbcomn_df(data_dict):
    vessel_500_orbcomn_df = pd.DataFrame(data_dict)
    today = datetime.datetime.now()
    # busan_arriving_vessels
    vessel_500_orbcomn_df["time_position_data_received"] = pd.to_datetime(
        vessel_500_orbcomn_df["time_position_data_received"].apply(lambda x: x.replace("T", " ").replace("Z", "")))
    vessel_500_orbcomn_df["time_voyage_data_received"] = pd.to_datetime(
        vessel_500_orbcomn_df["time_voyage_data_received"].apply(lambda x: x.replace("T", " ").replace("Z", "")))
    time_difference = vessel_500_orbcomn_df["time_position_data_received"] - vessel_500_orbcomn_df[
        "time_voyage_data_received"]
    vessel_500_orbcomn_df["time_difference"] = time_difference
    vessel_500_orbcomn_df["isnottoday"] = today - vessel_500_orbcomn_df["time_position_data_received"]
    vessel_500_orbcomn_df["timestamp"] = today
    # big_gap_df = vessel_500_orbcomn_df[vessel_500_orbcomn_df["isnottoday"] > timedelta(days=2)]
    # busan_arriving_vessels.drop(big_gap_df.index, inplace=True)
    for idx in vessel_500_orbcomn_df.index:
        if np.isnan(vessel_500_orbcomn_df.loc[idx, 'heading']):
            vessel_500_orbcomn_df.loc[idx, 'heading'] = 0
        if np.isnan(vessel_500_orbcomn_df.loc[idx, 'max_draught']):
            vessel_500_orbcomn_df.loc[idx, 'max_draught'] = 0

    vessel_500_orbcomn_df["destination"] = vessel_500_orbcomn_df["destination"].apply(lambda x: x.replace('"', ""))
    vessel_500_orbcomn_df["destination"] = vessel_500_orbcomn_df["destination"].apply(lambda x: x.replace("'", ""))
    vessel_500_orbcomn_df["s_id"] = 'seq_ais_acc_500.NEXTVAL'
    vessel_500_orbcomn_df["timestamp"] = vessel_500_orbcomn_df["timestamp"].apply(lambda x: pd.to_datetime(str(x)[:-7]))
    vessel_500_orbcomn_df.rename(columns={"vessel_name": "VSL_NM", "mmsi": 'MMSI'}, inplace=True)
    vessel_500_orbcomn_df.drop(columns=['imo', 'eta'], inplace=True)
    drop_idx = vessel_500_orbcomn_df[vessel_500_orbcomn_df['time_difference'] > timedelta(days=5)].index
    vessel_500_orbcomn_df.drop(drop_idx, inplace=True)
    vessel_500_orbcomn_df.reset_index(inplace=True, drop=True)

    drop_idx = vessel_500_orbcomn_df[vessel_500_orbcomn_df['isnottoday'] > timedelta(days=5)].index
    vessel_500_orbcomn_df.drop(drop_idx, inplace=True)
    vessel_500_orbcomn_df.reset_index(inplace=True, drop=True)

    return vessel_500_orbcomn_df
