import pandas as pd
import datetime
from shapely.geometry import Point, Polygon
from utils.util import *

def arrival_monitoring_table(tos_vessel, tos_plan_berth_atb_df, option_b_df, option_c_df,
                             vessel_voyage_report7days_df, vssl_optimal_routing_df, option_a_df, ais_accumulated_df):

    departing_vessels_idx_list = []
    on_vessels_idx_list = []
    for idx in tos_plan_berth_atb_df.index:
        VSL_CD = tos_plan_berth_atb_df.loc[idx, "VSL_CD"]
        VSL_NM = tos_vessel.query(f"VSL_CD == '{VSL_CD}'")["VSL_NM"].values[0]
        LOA = tos_vessel.query(f"VSL_CD == '{VSL_CD}'")["LOA"].values[0]

        tos_plan_berth_atb_df.loc[idx, "VSL_NM"] = VSL_NM
        tos_plan_berth_atb_df.loc[idx, "LOA"] = LOA

        ATD = tos_plan_berth_atb_df.loc[idx, "ATD"]
        if pd.isnull(ATD):
            on_vessels_idx_list.append(idx)
        else:
            departing_vessels_idx_list.append(idx)
    # 접안 중인 선박 df
    on_vessel_df = tos_plan_berth_atb_df.loc[on_vessels_idx_list, :]
    on_vessel_df.reset_index(inplace=True, drop=True)
    departing_vessel_df = tos_plan_berth_atb_df.loc[departing_vessels_idx_list, :]
    departing_vessel_df.reset_index(inplace=True, drop=True)
    arr_dict = {
        "VSL_CD": [],
        "VSL_NM": [],
        "VOYAGE": [],
        "REMAINED_DISTANCE": [],
        "FUEL_CONSUMPTION": [],
        "CO2_EMISSION": [],
        "BERTH": [],
        "MEAN_SPEED": [],
        "TOS_ETA": [],
        "ETA": [],
        "RTA": [],
        "PTA": [],
        "STATUS": []
    }
    for idx in on_vessel_df.index:
        VSL_CD = on_vessel_df.loc[idx, "VSL_CD"]
        VSL_NM = on_vessel_df.loc[idx, "VSL_NM"]
        VOYAGE = on_vessel_df.loc[idx, "VOYAGE"]
        TOS_ETA = on_vessel_df.loc[idx, "ETA"]
        STATUS = "BERTHED"
        RTA = option_b_df.query(f"VOYAGE == '{VOYAGE}'")["RTA"].values[0]
        PTA = option_c_df.query(f"VOYAGE == '{VOYAGE}'")["PTA"].values[0]
        BERTH = option_c_df.query(f"VOYAGE == '{VOYAGE}'")["BERTH_C"].values[0]
        arr_dict['VSL_CD'].append(VSL_CD)
        arr_dict['VSL_NM'].append(VSL_NM)
        arr_dict['VOYAGE'].append(VOYAGE)
        arr_dict['TOS_ETA'].append(TOS_ETA)
        arr_dict['STATUS'].append(STATUS)
        arr_dict['RTA'].append(RTA)
        arr_dict['BERTH'].append(BERTH)
        arr_dict['PTA'].append(PTA)

        try:
            vessel_voyage_report = vessel_voyage_report7days_df.query(
                f"VOYAGE == '{VOYAGE}'")
            FUEL_CONSUMPTION = vessel_voyage_report[
                'FINAL_ESTIMATED_FUEL_CONSUMPTION'].values[0]
            CO2_EMISSION = vessel_voyage_report['FINAL_CO2_EMISSION'].values[0]
            MEAN_SPEED = vessel_voyage_report['MEAN_SPEED'].values[0]
            ETA = vessel_voyage_report['PREDICTED_ETA'].values[0]

            arr_dict['ETA'].append(ETA)
            arr_dict['FUEL_CONSUMPTION'].append(FUEL_CONSUMPTION)
            arr_dict['CO2_EMISSION'].append(CO2_EMISSION)
            arr_dict['MEAN_SPEED'].append(MEAN_SPEED)
            arr_dict['REMAINED_DISTANCE'].append(0)

        except:
            arr_dict['ETA'].append("")
            arr_dict['FUEL_CONSUMPTION'].append(0)
            arr_dict['CO2_EMISSION'].append(0)
            arr_dict['MEAN_SPEED'].append(0)
            arr_dict['REMAINED_DISTANCE'].append(0)
    for idx in vssl_optimal_routing_df.index:
        VSL_CD = vssl_optimal_routing_df.loc[idx, "VSL_CD"]
        VSL_NM = vssl_optimal_routing_df.loc[idx, "VSSL_ENG_NM"]
        VOYAGE = option_a_df.query(f"VSL_CD == '{VSL_CD}'")['VOYAGE'].values[0]

        FUEL_CONSUMPTION = vssl_optimal_routing_df.loc[idx, "VSSL_FOC"]
        CO2_EMISSION = vssl_optimal_routing_df.loc[idx, "VSSL_CO2"]
        MEAN_SPEED = vssl_optimal_routing_df.loc[idx, "MEAN_SPEED"]
        ETA = vssl_optimal_routing_df.loc[idx, "ETA_ANALYSIS"]
        REMAINED_DISTANCE = vssl_optimal_routing_df.loc[idx, "REMAINED_DISTANCE"]

        TOS_ETA = vssl_optimal_routing_df.loc[idx, "ETA_PORT_TOS"]
        try:
            RTA = option_b_df.query(f"VOYAGE == '{VOYAGE}'")["RTA"].values[0]
            PTA = option_c_df.query(f"VOYAGE == '{VOYAGE}'")["PTA"].values[0]
            BERTH = option_c_df.query(f"VOYAGE == '{VOYAGE}'")["BERTH_C"].values[0]
        except:
            BERTH = "OT"
            RTA = pd.to_datetime("2022-01-01 00:00:00")
            PTA = pd.to_datetime("2022-01-01 00:00:00")
        if abs(ETA - TOS_ETA) < datetime.timedelta(hours=1):
            STATUS = "ON_TIME"
        elif ETA > TOS_ETA:
            STATUS = "DELAY"
        else:
            STATUS = "FASTER THAN EXPECTED"

        arr_dict['VSL_CD'].append(VSL_CD)
        arr_dict['VSL_NM'].append(VSL_NM)
        arr_dict['VOYAGE'].append(VOYAGE)
        arr_dict['TOS_ETA'].append(TOS_ETA)
        arr_dict['STATUS'].append(STATUS)
        arr_dict['RTA'].append(RTA)
        arr_dict['PTA'].append(PTA)
        arr_dict['ETA'].append(ETA)
        arr_dict['BERTH'].append(BERTH)

        arr_dict['FUEL_CONSUMPTION'].append(FUEL_CONSUMPTION)
        arr_dict['CO2_EMISSION'].append(CO2_EMISSION)
        arr_dict['MEAN_SPEED'].append(MEAN_SPEED)
        arr_dict['REMAINED_DISTANCE'].append(REMAINED_DISTANCE)
    pnit_real_anchorage_envelop = '128.7252142651504,34.99135571955362,5.63702384267877 128.7177574560664,34.95934264413938,5.676714574792752 128.7487815911821,34.84824211017854,-9.216155089598427 128.8640201038309,34.8458701526411,-9.431495817641029 128.9034765347025,34.84558606109609,-9.820382569000833 128.9409079747262,34.848755038238,6.05722408107185 128.960691950853,34.86176891005817,5.500784674410049 128.9756776610653,34.88200246986016,5.75976629477773 128.9508889169689,34.91375398027574,5.445836776829427 128.9638610398305,34.92962587176246,5.660894905512743 128.9720895663599,34.94178298012624,5.816586589856241 128.9783211592115,34.95249547765555,5.724139684827854 128.9784929567112,34.97203596062337,5.878958555158547 128.9809501366994,34.98603362167294,5.952477437737709 128.9816516337907,35.00002925693838,5.676023325243291 128.9790835296531,35.01442976238769,5.866476451698455 128.9755140163752,35.02759547585965,5.970723316729214 128.9683465517219,35.03947445801186,36.53153259528653 128.9633703222173,35.04650435808495,5.43976181401657 128.9278776294942,35.0523501720843,5.587465113245768 128.896164173877,35.05655515745237,5.714948395887407 128.8677085677609,35.06200497820917,5.658995270374591 128.8496891412019,35.05279330567865,44.98509217259041 128.8480231194191,35.03676151912366,26.25045353446817 128.8398728135739,35.01307619527701,5.655596530913982 128.8136145895992,34.99587793108583,5.55140332774092 128.7252142651504,34.99135571955362,5.63702384267877'
    # pnit_anchorage_envelop = '128.7401543185474,35.07996524145801,9.457948242456919 128.7081705615012,34.99231468116388,-8.879754028257278 128.7211960378933,34.71260343418253,-2.896038151998478 129.1177509358209,34.90506823658274,5.249339927241989 129.0004979368669,35.06217241043465,7.854346247709958 128.8268259657758,35.08756898986205,8.355275782056271 128.7401543185474,35.07996524145801,9.457948242456919'
    coord_list = pnit_real_anchorage_envelop.split(" ")
    coord_list2 = []
    for i in coord_list:
        temp = i.split(",")[:2]
        temp = list(map(float, temp))
        coord_list2.append(temp)
    poly = Polygon(coord_list2)
    vessels_real_anchoring_at_busan_list = []
    for idx in ais_accumulated_df.index:
        TOS_VSL_NM = ais_accumulated_df.loc[idx, "TOS_VSL_NM"]
        destination = ais_accumulated_df.loc[idx, "DESTINATION"]
        LONGITUDE = ais_accumulated_df.loc[idx, "LONGITUDE"]
        LATITUDE = ais_accumulated_df.loc[idx, "LATITUDE"]
        NAVIGATION_INFO = ais_accumulated_df.loc[idx, "NAVIGATION_INFO"]
        # pnit 앵커리지 근처에 위치가 찍혔을 때
        if Point(LONGITUDE, LATITUDE).within(poly):
            vessels_real_anchoring_at_busan_list.append(TOS_VSL_NM)
    #     elif NAVIGATION_INFO != 'under way using engine':
    #         if ('KR' in destination) or ("BUS" in destination) or ("PUS" in destination):
    #             vessels_anchoring_at_busan_list.append(TOS_VSL_NM)
    on_vessel_list = on_vessel_df["VSL_NM"].values
    vessel_voyage_report_df = vessel_voyage_report7days_df.copy()
    for vsl_nm in on_vessel_list:
        drop_idx = vessel_voyage_report_df.query(f'TOS_VSL_NM == "{vsl_nm}"').index
        vessel_voyage_report_df.drop(drop_idx)
    vessel_voyage_report_df.reset_index(inplace=True, drop=True)
    for vsl_nm in vessels_real_anchoring_at_busan_list:
        df = vessel_voyage_report_df.query(f"TOS_VSL_NM == '{vsl_nm}'")
        if len(df) == 0:
            continue

        VSL_CD = df["VSL_CD"].values[0]
        VSL_NM = df["VSL_NM"].values[0]
        VOYAGE = df["VOYAGE"].values[0]
        TOS_ETA = df["TOS_ETA"].values[0]
        STATUS = "ANCHORING"
        RTA = option_b_df.query(f"VOYAGE == '{VOYAGE}'")["RTA"].values[0]
        PTA = option_c_df.query(f"VOYAGE == '{VOYAGE}'")["PTA"].values[0]
        BERTH = option_c_df.query(f"VOYAGE == '{VOYAGE}'")["BERTH_C"].values[0]
        FUEL_CONSUMPTION = df['FINAL_ESTIMATED_FUEL_CONSUMPTION'].values[0]
        CO2_EMISSION = df['FINAL_CO2_EMISSION'].values[0]
        MEAN_SPEED = df['MEAN_SPEED'].values[0]
        ETA = df['PREDICTED_ETA'].values[0]

        arr_dict['VSL_CD'].append(VSL_CD)
        arr_dict['VSL_NM'].append(VSL_NM)
        arr_dict['VOYAGE'].append(VOYAGE)
        arr_dict['TOS_ETA'].append(TOS_ETA)
        arr_dict['STATUS'].append(STATUS)
        arr_dict['RTA'].append(RTA)
        arr_dict['BERTH'].append(BERTH)
        arr_dict['PTA'].append(PTA)
        arr_dict['ETA'].append(ETA)
        arr_dict['FUEL_CONSUMPTION'].append(FUEL_CONSUMPTION)
        arr_dict['CO2_EMISSION'].append(CO2_EMISSION)
        arr_dict['MEAN_SPEED'].append(MEAN_SPEED)
        arr_dict['REMAINED_DISTANCE'].append(0)
    arrival_monitoring = pd.DataFrame(arr_dict)
    today = datetime.datetime.today()
    arrival_monitoring["TIMESTAMP"] = today
    arrival_monitoring["TIMESTAMP"] = arrival_monitoring["TIMESTAMP"].apply(
        lambda x: pd.to_datetime(str(x)[:-7])
        if len(str(x)) > 19 else pd.to_datetime(x))
    return arrival_monitoring, on_vessel_df

def departure_monitoring_table(on_vessel_df, url, tos_plan_berth_atd_df):
    working_time_df = calculate_terminal_working_time(on_vessel_df, url)
    new_on_vessel_df = pd.merge(on_vessel_df, working_time_df, how="outer", on="VOYAGE" )
    new_on_vessel_df['ETD_updated'] = new_on_vessel_df['ATB'] + new_on_vessel_df['work']
    new_on_vessel_df.loc[new_on_vessel_df.eval(f'ETD_updated > ETD'), "STATUS"] = "DELAY"
    new_on_vessel_df.loc[new_on_vessel_df.eval(f'ETD_updated < ETD'), "STATUS"] = "FASTER_THAN_EXPECTED"
    new_on_vessel_df.loc[new_on_vessel_df.eval(f'abs(ETD_updated - ETD) < "{datetime.timedelta(hours=1)}"'), "STATUS"] = "ON_TIME"
    dep_dict = {
        "VSL_CD": [],
        "VSL_NM": [],
        "VOYAGE": [],
        "DWELL_TIME": [],
        "TOS_ETD": [],
        "BERTH": [],
        "ETD": [],
        "STATUS": []
    }
    for idx in new_on_vessel_df.index:

        VSL_CD = new_on_vessel_df.loc[idx, "VSL_CD"]
        VSL_NM = new_on_vessel_df.loc[idx, "VSL_NM"]
        VOYAGE = new_on_vessel_df.loc[idx, "VOYAGE"]
        DWELL_TIME = new_on_vessel_df.loc[idx, "work"]
        TOS_ETD = new_on_vessel_df.loc[idx, "ETD"]
        ETD = new_on_vessel_df.loc[idx, "ETD_updated"]
        BERTH = new_on_vessel_df.loc[idx, "BERTH_NO"]

        STATUS = new_on_vessel_df.loc[idx, "STATUS"]

        dep_dict['VSL_CD'].append(VSL_CD)
        dep_dict['VSL_NM'].append(VSL_NM)
        dep_dict['VOYAGE'].append(VOYAGE)
        dep_dict['BERTH'].append(BERTH)
        dep_dict['DWELL_TIME'].append(DWELL_TIME)
        dep_dict['STATUS'].append(STATUS)
        dep_dict['TOS_ETD'].append(TOS_ETD)
        dep_dict['ETD'].append(ETD)

    tos_plan_berth_atd_df['work'] = tos_plan_berth_atd_df['ATD'] - tos_plan_berth_atd_df["ATB"]

    for idx in tos_plan_berth_atd_df.index:

        VSL_CD = tos_plan_berth_atd_df.loc[idx, "VSL_CD"]
        VSL_NM = tos_plan_berth_atd_df.loc[idx, "VSL_NM"]
        VOYAGE = tos_plan_berth_atd_df.loc[idx, "VOYAGE"]
        DWELL_TIME = tos_plan_berth_atd_df.loc[idx, "work"]
        TOS_ETD = tos_plan_berth_atd_df.loc[idx, "ETD"]
        ETD = tos_plan_berth_atd_df.loc[idx, "ATD"]
        BERTH = tos_plan_berth_atd_df.loc[idx, "BERTH_NO"]

        STATUS = "AWAY"

        dep_dict['VSL_CD'].append(VSL_CD)
        dep_dict['VSL_NM'].append(VSL_NM)
        dep_dict['BERTH'].append(BERTH)
        dep_dict['VOYAGE'].append(VOYAGE)
        dep_dict['DWELL_TIME'].append(DWELL_TIME)
        dep_dict['STATUS'].append(STATUS)
        dep_dict['TOS_ETD'].append(TOS_ETD)
        dep_dict['ETD'].append(ETD)
    departure_monitoring = pd.DataFrame(dep_dict)
    today = datetime.datetime.today()
    departure_monitoring["TIMESTAMP"] = today
    departure_monitoring["TIMESTAMP"] = departure_monitoring["TIMESTAMP"].apply(
        lambda x: pd.to_datetime(str(x)[:-7])
        if len(str(x)) > 19 else pd.to_datetime(x))
    departure_monitoring["ETD"] = departure_monitoring["ETD"].apply(
        lambda x: pd.to_datetime(str(x)[:-7])
        if len(str(x)) > 19 else pd.to_datetime(x))

    return departure_monitoring
