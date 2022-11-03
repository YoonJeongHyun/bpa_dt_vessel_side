import pandas as pd
from pyproj import Geod
import datetime
import numpy as np


def optimal_routing_prepartion(ais_data_accumulated, tos_vessel, tos_plan_berth_df):
    ais_data_accumulated = ais_data_accumulated.query('NAVIGATION_INFO == "under way using engine"')
    ais_data_accumulated.reset_index(drop=True, inplace=True)
    dict_ = {"VSL_NM": [], "CLSGN": [], "VSL_CD": []}
    for vsl_nm in ais_data_accumulated["TOS_VSL_NM"].values:
        df0 = tos_vessel.query(f'VSL_NM == "{vsl_nm}"')
        call_sign = df0["CALL_SIGN"].values[0]
        vsl_cd = df0["VSL_CD"].values[0]
        dict_["VSL_NM"].append(vsl_nm)
        dict_["CLSGN"].append(call_sign)
        dict_["VSL_CD"].append(vsl_cd)
    df1 = pd.DataFrame(dict_)

    dict__ = {"VSL_CD": [], "SERNO": [], "YEAR": [], "ETA_PORT_TOS": []}
    for code in df1["VSL_CD"].values:
        df2 = tos_plan_berth_df.query(f'VSL_CD == "{code}"')
        #     display(df2)
        try:
            SERNO = df2["CALL_SEQ"].values[0]
            VSL_CD = df2["VSL_CD"].values[0]
            YEAR = df2["CALL_YEAR"].values[0]
            ETA_PORT_TOS = df2["ETA"].values[0]
            dict__["SERNO"].append(SERNO)
            dict__["YEAR"].append(YEAR)
            dict__["VSL_CD"].append(VSL_CD)
            dict__["ETA_PORT_TOS"].append(ETA_PORT_TOS)
        except:
            pass

    df3 = pd.DataFrame(dict__)
    df4 = pd.merge(df3, df1, how='inner')
    op_routing_df = pd.merge(df4, ais_data_accumulated, how='inner')
    op_routing_df = op_routing_df.query(
        "DESTINATION.str.contains('BUS') | DESTINATION.str.contains('PUS') | DESTINATION.str.contains('BNP')")
    op_routing_df.reset_index(inplace=True, drop=True)
    op_routing_df["OR_ID"] = 'seq_vssl_optimal_routing.NEXTVAL'
    op_routing_df.drop([
        "DESTINATION", 'NAVIGATION_INFO', 'TIME_POSITION_DATA_RECEIVED',
        'TIME_VOYAGE_DATA_RECEIVED', 'CAPACITY', 'TIME_DIFFERENCE', 'ISNOTTODAY'
    ],
        axis=1,
        inplace=True)

    return op_routing_df


def calculate_routes(PNIT_anchorage, tug_static_area_df, ais_data_accumulated, tos_vessel, tos_plan_berth_df):
    op_routing_df = optimal_routing_prepartion(ais_data_accumulated, tos_vessel, tos_plan_berth_df)
    for i in range(1, 21):
        op_routing_df[f"LAT{i}"] = 1.
        op_routing_df[f"LON{i}"] = 1.
        op_routing_df[f"TIME{i}"] = ""
        op_routing_df[f"HDG{i}"] = 1.
        op_routing_df[f"ARRIVING_ROUTE_LAT{i}"] = 1.
        op_routing_df[f"ARRIVING_ROUTE_LON{i}"] = 1.

    for idx in op_routing_df.index:
        longitude = op_routing_df.loc[idx, 'LONGITUDE']
        latitude = op_routing_df.loc[idx, 'LATITUDE']
        TIMESTAMP = op_routing_df.loc[idx, "TIMESTAMP"]
        eta = op_routing_df.loc[idx, "PREDICTED_ETA"]
        n_extra_points = 20
        geoid = Geod(ellps='WGS84')
        extra_points = geoid.npts(longitude, latitude, PNIT_anchorage[1],
                                  PNIT_anchorage[0], n_extra_points)
        for i, point in enumerate(extra_points):
            c = eta - TIMESTAMP
            durations_in_seconds = c / np.timedelta64(1, 's')
            durations = abs(durations_in_seconds / 20 * (i + 1))
            ETA_SPLIT = TIMESTAMP + datetime.timedelta(seconds=durations)
            op_routing_df.loc[idx, f"LAT{i + 1}"] = point[1]
            op_routing_df.loc[idx, f"LON{i + 1}"] = point[0]
            op_routing_df.loc[idx, f"TIME{i + 1}"] = ETA_SPLIT
            op_routing_df.loc[idx, f"ARRIVING_ROUTE_LAT{i + 1}"] = tug_static_area_df.loc[i, "LAT"]
            op_routing_df.loc[idx, f"ARRIVING_ROUTE_LON{i + 1}"] = tug_static_area_df.loc[i, "LON"]

    op_routing_df['VSSL_CO2'] = op_routing_df['ESTIMATED_FUEL_CONSUMPTION'].apply(lambda x: 3.11440 * x)
    op_routing_df['IBOBPRT_SE'] = 1
    op_routing_df['ERROR'] = 1.
    op_routing_df["ATA"] = "2111-12-31 11:59:59"
    op_routing_df.rename(
        {
            "LONGITUDE": "CRRNT_LON",
            'LATITUDE': 'CRRNT_LAT',
            "HEADING": "CRRNT_HDG",
            "TIMESTAMP": "CRRNT_TIME",
            "SPEED": "MEAN_SPEED",
            "PREDICTED_ETA": "ETA_ANALYSIS",
            "ESTIMATED_FUEL_CONSUMPTION": "VSSL_FOC",
            "YEAR": "YR",
            "VSL_NM": "VSSL_ENG_NM",
            "MMSI": "VSSL_MMSI"
        },
        axis=1,
        inplace=True)

    return op_routing_df
