import pandas as pd
import datetime
from datetime import timedelta
from utils.util import *


def option_A_table(tos_plan_berth_df, tos_vessel):
    for idx in tos_plan_berth_df.index:
        vsl_cd = tos_plan_berth_df.loc[idx, "VSL_CD"]
        df0 = tos_vessel.query(f'VSL_CD == "{vsl_cd}"')
        VSL_CD = df0["VSL_CD"].values[0]
        VSL_NM = df0["VSL_NM"].values[0]
        LOA = df0["LOA"].values[0]
        WIDTH = df0["WIDTH"].values[0]
        tos_plan_berth_df.loc[idx, "VSL_CD"] = VSL_CD
        tos_plan_berth_df.loc[idx, "VSL_NM"] = VSL_NM
        tos_plan_berth_df.loc[idx, "LOA"] = LOA
        tos_plan_berth_df.loc[idx, "WIDTH"] = WIDTH

    tos_plan_berth_df["a_id"] = "seq_berth_a.NEXTVAL"
    original_tos_plan_berth_a_df = tos_plan_berth_df.copy()
    original_tos_plan_berth_a_df.rename(columns={
        'BERTH_NO': 'BERTH_A',
        'FROM_BITT': 'FROM_BITT_A',
        'TO_BITT': 'TO_BITT_A',
        'CALL_YEAR': 'YR',
        'CALL_SEQ': "SER_NO"
    },
        inplace=True)
    original_tos_plan_berth_a_df["timestamp"] = original_tos_plan_berth_a_df["timestamp"].apply(
        lambda x: pd.to_datetime(str(x)[:-7]))

    return original_tos_plan_berth_a_df


def option_B_table(tos_plan_berth_df, tos_vessel):
    # option b
    initplan = tos_plan_berth_df.sort_values(by='ETB')
    initplan = initplan.loc[:, [
                                   'VSL_CD', 'VOYAGE', 'BERTH_NO', 'ALONG_SIDE', 'FROM_BITT', 'TO_BITT',
                                   'LOAD_QTY', 'DIS_QTY', 'ETA', 'ETB', 'ETD'
                               ]]
    initplan.FROM_BITT = pd.to_numeric(initplan.FROM_BITT,
                                       errors='coerce').fillna(100)
    initplan.TO_BITT = pd.to_numeric(initplan.TO_BITT, errors='coerce').fillna(100)
    initplan['bitt'] = initplan['TO_BITT'] - initplan['FROM_BITT']
    initplan['work'] = initplan['ETD'] - initplan['ETB']
    vesseldata = tos_vessel.loc[:, ['VSL_CD', 'VSL_NM', 'LOA', 'WIDTH']]
    plan_data = pd.merge(initplan, vesseldata, on='VSL_CD', how='left')
    for idx in plan_data.index:
        LOA = plan_data.loc[idx, "LOA"]
        bitt = plan_data.loc[idx, "bitt"]
        if (bitt == 0.0) or (bitt >= 24.0) or (bitt <= -24.0):
            if LOA <= 11250:
                plan_data.loc[idx, 'bitt'] = 9
            elif LOA <= 15000:
                plan_data.loc[idx, 'bitt'] = 10
            elif LOA <= 17500:
                plan_data.loc[idx, 'bitt'] = 11
            elif LOA < 18000:
                plan_data.loc[idx, 'bitt'] = 12
            elif LOA < 21000:
                plan_data.loc[idx, 'bitt'] = 13
            elif LOA < 25000:
                plan_data.loc[idx, 'bitt'] = 14
            elif LOA < 30000:
                plan_data.loc[idx, 'bitt'] = 19
            else:
                plan_data.loc[idx, 'bitt'] = 23

    ##한 선석당 2척의 선박이 접안될 때 사용하는 비트는 25개
    plan_data['FROM_BITT'] = 0
    plan_data['TO_BITT'] = 0
    t1 = plan_data[plan_data['BERTH_NO'] == 'T1']
    t2 = plan_data[plan_data['BERTH_NO'] == 'T2']
    t3 = plan_data[plan_data['BERTH_NO'] == 'T3']
    ot = plan_data[plan_data['BERTH_NO'] == 'OT']
    t1 = t1.drop_duplicates(['VOYAGE'], keep='first')
    t2 = t2.drop_duplicates(['VOYAGE'], keep='first')
    t3 = t3.drop_duplicates(['VOYAGE'], keep='first')
    t1 = t1.reset_index(drop=True)
    t2 = t2.reset_index(drop=True)
    t3 = t3.reset_index(drop=True)
    t1['dupl'] = 0
    t2['dupl'] = 0
    t3['dupl'] = 0
    # t1=t1.reindex(['VSL_CD', 'VOYAGE', 'BERTH_NO', 'dupl', 'FROM_BITT', 'TO_BITT', 'ETA', 'ETB', 'ETD', 'bitt', 'work', 'VSL_NM', 'LOA'], axis=1)
    # t2=t2.reindex(['VSL_CD', 'VOYAGE', 'BERTH_NO', 'dupl', 'FROM_BITT', 'TO_BITT', 'ETA', 'ETB', 'ETD', 'bitt', 'work', 'VSL_NM', 'LOA'], axis=1)
    # t3=t3.reindex(['VSL_CD', 'VOYAGE', 'BERTH_NO', 'dupl', 'FROM_BITT', 'TO_BITT', 'ETA', 'ETB', 'ETD', 'bitt', 'work', 'VSL_NM', 'LOA'], axis=1)
    berth_list = [t1, t2, t3]
    for berth in berth_list:
        for idx in berth.index:
            # 제일 처음 선박 제외
            if idx == 0:
                continue

            # 앞 선박의 ETD + 2시간보다 ETB가 빠른 선박들 확인
            first_ship_ETB = berth.loc[idx - 1, "ETB"]
            second_ship_ETB = berth.loc[idx, "ETB"]

            first_ship_ETD = berth.loc[idx - 1, "ETD"]

            first_ship_bitt = berth.loc[idx - 1, "bitt"]
            second_ship_bitt = berth.loc[idx, "bitt"]

            working_hour = berth.loc[idx, "work"]

            if second_ship_ETB < first_ship_ETD + timedelta(hours=2):

                # 한 선석에서 두 척 접안할 수 있는 경우
                # 한 선석의 최대 이용 가능 비트는 25이다.
                # 한 선석에 연쇄적으로 접안할 선박 2개를 비교하여 (차지 bitt 수) 25 이하인 선박들을 한 선석에 2개 넣는다.

                if first_ship_bitt + second_ship_bitt <= 25:
                    new_second_ship_ETB = first_ship_ETB
                    new_second_ship_ETD = new_second_ship_ETB + working_hour

                    berth.loc[idx, "ETB"] = new_second_ship_ETB
                    berth.loc[idx, "ETD"] = new_second_ship_ETD

                    # 한 선석에 붙을 선박 2척 중 나중에 오는 선박이 먼저 접안 할 선박보다 빨리 출항(ETD가 빠른)하는 경우
                    # 두 선박의 데이터프레임상의 위치 바꾸기
                    if new_second_ship_ETD < first_ship_ETD:
                        temp = berth.loc[idx - 1].copy()
                        berth.loc[idx - 1] = berth.loc[idx]
                        berth.loc[idx] = temp
                    # 2척 접안하는 선박의 의미인 dupl이 1로 변경
                    berth.loc[idx, "dupl"] = 1

                # 한 선석에 1개의 선박이 접안하는 경우
                else:
                    new_second_ship_ETB = first_ship_ETD + timedelta(hours=2)
                    new_second_ship_ETD = new_second_ship_ETB + working_hour

                    berth.loc[idx, "ETB"] = new_second_ship_ETB
                    berth.loc[idx, "ETD"] = new_second_ship_ETD

    plan_b = pd.concat([t1, t2, t3], ignore_index=True)
    plan_b = plan_b.sort_values(by='ETB')
    plan_b = plan_b.reset_index(drop=True)
    t1_new = t1
    t2_new = t2
    t3_new = t3
    t1_new['real_FROM_BITT'] = 0
    t1_new['real_TO_BITT'] = 0
    t2_new['real_FROM_BITT'] = 0
    t2_new['real_TO_BITT'] = 0
    t3_new['real_FROM_BITT'] = 0
    t3_new['real_TO_BITT'] = 0
    for q1, row in t1_new.iterrows():
        if t1_new.dupl[q1] == 0:
            t1_new.real_FROM_BITT[q1] = 1
            t1_new.real_TO_BITT[q1] = t1_new.real_FROM_BITT[q1] + abs(t1_new.bitt[q1])
        else:
            t1_new.real_FROM_BITT[q1] = t1_new.real_TO_BITT[q1 - 1] - 1
            t1_new.real_TO_BITT[q1] = t1_new.real_FROM_BITT[q1] + abs(t1_new.bitt[q1])

    for w1, row in t2_new.iterrows():
        b1 = t1_new[t1_new['ETB'] <= t2_new.ETD[w1]]
        b1 = b1[b1['ETD'] >= t2_new.ETB[w1]]

        if b1.empty == True:
            if t2_new.dupl[w1] == 0:
                t2_new.real_FROM_BITT[w1] = 23
                t2_new.real_TO_BITT[w1] = t2_new.real_FROM_BITT[w1] + abs(t2_new.bitt[w1])
            else:
                t2_new.real_FROM_BITT[w1] = t2_new.real_TO_BITT[w1 - 1] - 1
                t2_new.real_TO_BITT[w1] = t2_new.real_FROM_BITT[w1] + abs(t2_new.bitt[w1])

        else:
            if t2_new.dupl[w1] == 0:
                t2_new.real_FROM_BITT[w1] = b1.max()['real_TO_BITT'] - 1
                t2_new.real_TO_BITT[w1] = t2_new.real_FROM_BITT[w1] + abs(t2_new.bitt[w1])
            else:
                t2_new.real_FROM_BITT[w1] = t2_new.real_TO_BITT[w1 - 1] - 1
                t2_new.real_TO_BITT[w1] = t2_new.real_FROM_BITT[w1] + abs(t2_new.bitt[w1])

    for e1, row in t3_new.iterrows():
        b2 = t2_new[t2_new['ETB'] <= t3_new.ETD[e1]]
        b2 = b2[b2['ETD'] >= t3_new.ETB[e1]]
        # b2 = b2.tail(n=1)
        # b2 = b2.reset_index(drop=True)

        if b2.empty == True:
            if t3_new.dupl[e1] == 0:
                t3_new.real_TO_BITT[e1] = 63
                t3_new.real_FROM_BITT[e1] = t3_new.real_TO_BITT[e1] - abs(
                    t3_new.bitt[e1])
            else:
                t3_new.real_TO_BITT[e1] = t3_new.real_FROM_BITT[e1 - 1] + 1
                t3_new.real_FROM_BITT[e1] = t3_new.real_TO_BITT[e1] - abs(
                    t3_new.bitt[e1])

        else:
            if t3_new.dupl[e1] == 0:
                t3_new.real_FROM_BITT[e1] = b2.max()['real_TO_BITT'] - 1
                t3_new.real_TO_BITT[e1] = t3_new.real_FROM_BITT[e1] + abs(
                    t3_new.bitt[e1])
            else:
                t3_new.real_FROM_BITT[e1] = t3_new.real_TO_BITT[e1 - 1] - 1
                t3_new.real_TO_BITT[e1] = t3_new.real_FROM_BITT[e1] + abs(
                    t3_new.bitt[e1])

    plan_b_final = pd.concat([t1_new, t2_new, t3_new], ignore_index=True)
    plan_b_final = plan_b_final.sort_values(by='ETB')
    plan_b_final = plan_b_final.reset_index(drop=True)
    plan_b_final['FROM_BITT'] = plan_b_final['real_FROM_BITT'] + 1
    plan_b_final['TO_BITT'] = plan_b_final['real_TO_BITT'] - 1
    for r, row in plan_b_final.iterrows():
        if plan_b_final.ALONG_SIDE[r] != 'P':
            bittnum = plan_b_final.real_TO_BITT[q1].copy()
            plan_b_final.real_TO_BITT[q1] = plan_b_final.real_FROM_BITT[q1]
            plan_b_final.real_FROM_BITT[q1] = bittnum
            viewnum = plan_b_final.TO_BITT[q1].copy()
            plan_b_final.TO_BITT[q1] = plan_b_final.FROM_BITT[q1]
            plan_b_final.FROM_BITT[q1] = bittnum
    plan_b_final = plan_b_final.loc[:, [
                                           'VOYAGE', 'VSL_NM', 'BERTH_NO', 'FROM_BITT', 'TO_BITT', 'ETB', 'ETD',
                                           'ALONG_SIDE', 'DIS_QTY', 'LOAD_QTY', 'LOA', 'WIDTH'
                                       ]]
    add = tos_plan_berth_df.loc[:, [
                                       'VOYAGE', 'VSL_CD', 'CALL_YEAR', 'CALL_SEQ', 'PTNR_CODE', 'timestamp'
                                   ]]
    b_final = pd.merge(plan_b_final, add, on='VOYAGE', how='left')
    b_final = b_final.reindex([
        'VSL_CD', 'VOYAGE', 'CALL_YEAR', 'CALL_SEQ', 'VSL_NM', 'PTNR_CODE',
        'BERTH_NO', 'FROM_BITT', 'TO_BITT', 'ETB', 'ETD', 'ALONG_SIDE', 'DIS_QTY',
        'LOAD_QTY', 'LOA', 'WIDTH', 'timestamp'
    ],
        axis=1)
    b_final.columns = [
        'VSL_CD', 'VOYAGE', 'YR', 'SER_NO', 'VSSL_NM', 'PTNR_CODE', 'BERTH_B',
        'FROM_BITT_B', 'TO_BITT_B', 'RTA', 'RTD', 'ALONG_SIDE', 'DIS_QTY',
        'LOAD_QTY', 'LOA', 'WIDTH', 'TIMESTAMP'
    ]
    b_final["b_id"] = "seq_berth_a.NEXTVAL"
    b_final.rename(columns={
        "VSSL_NM": "VSL_NM",
        'TIMESTAMP': "timestamp"
    },
        inplace=True)
    b_final["timestamp"] = b_final["timestamp"].apply(
        lambda x: pd.to_datetime(str(x)[:-7]))
    return b_final


def option_C_table(url, tos_plan_berth_df, tos_vessel, ais_data_accumulated):
    # option c
    initplan = tos_plan_berth_df.sort_values(by='ETB')
    initplan = initplan.loc[:,
               ['VSL_CD', 'VOYAGE', 'BERTH_NO', 'ALONG_SIDE', 'FROM_BITT', 'TO_BITT', 'LOAD_QTY', 'DIS_QTY', 'ETA',
                'ETB', 'ETD', "LOA"]]
    initplan.FROM_BITT = pd.to_numeric(initplan.FROM_BITT, errors='coerce').fillna(100)
    initplan.TO_BITT = pd.to_numeric(initplan.TO_BITT, errors='coerce').fillna(100)

    working_time_df = calculate_terminal_working_time(initplan, url)
    initplan = pd.merge(initplan, working_time_df, on="VOYAGE", how='left')
    initplan["ETD"] = initplan["ETB"] + initplan["work"]

    initplan['bitt'] = initplan['TO_BITT'] - initplan['FROM_BITT']
    # initplan['work'] = initplan['ETD'] - initplan['ETB']
    vesseldata = tos_vessel.loc[:, ['VSL_CD', 'VSL_NM', 'WIDTH']]
    plan_data = pd.merge(initplan, vesseldata, on='VSL_CD', how='left')
    for idx in plan_data.index:
        LOA = plan_data.loc[idx, "LOA"]
        bitt = plan_data.loc[idx, "bitt"]
        if (bitt == 0.0) or (bitt >= 24.0) or (bitt <= -24.0):
            if LOA <= 11250:
                plan_data.loc[idx, 'bitt'] = 9
            elif LOA <= 15000:
                plan_data.loc[idx, 'bitt'] = 10
            elif LOA <= 17500:
                plan_data.loc[idx, 'bitt'] = 11
            elif LOA < 18000:
                plan_data.loc[idx, 'bitt'] = 12
            elif LOA < 21000:
                plan_data.loc[idx, 'bitt'] = 13
            elif LOA < 25000:
                plan_data.loc[idx, 'bitt'] = 14
            elif LOA < 30000:
                plan_data.loc[idx, 'bitt'] = 19
            else:
                plan_data.loc[idx, 'bitt'] = 23

    ##한 선석당 2척의 선박이 접안될 때 사용하는 비트는 25개
    plan_data['FROM_BITT'] = 0
    plan_data['TO_BITT'] = 0
    t1 = plan_data[plan_data['BERTH_NO'] == 'T1']
    t2 = plan_data[plan_data['BERTH_NO'] == 'T2']
    t3 = plan_data[plan_data['BERTH_NO'] == 'T3']
    ot = plan_data[plan_data['BERTH_NO'] == 'OT']
    t1 = t1.drop_duplicates(['VOYAGE'], keep='first')
    t2 = t2.drop_duplicates(['VOYAGE'], keep='first')
    t3 = t3.drop_duplicates(['VOYAGE'], keep='first')
    t1 = t1.reset_index(drop=True)
    t2 = t2.reset_index(drop=True)
    t3 = t3.reset_index(drop=True)
    t1['dupl'] = 0
    t2['dupl'] = 0
    t3['dupl'] = 0

    berth_list = [t1, t2, t3]
    for berth in berth_list:
        for idx in berth.index:
            # 제일 처음 선박 제외
            if idx == 0:
                continue

            # 앞 선박의 ETD + 2시간보다 ETB가 빠른 선박들 확인
            first_ship_ETB = berth.loc[idx - 1, "ETB"]
            second_ship_ETB = berth.loc[idx, "ETB"]

            first_ship_ETD = berth.loc[idx - 1, "ETD"]

            first_ship_bitt = berth.loc[idx - 1, "bitt"]
            second_ship_bitt = berth.loc[idx, "bitt"]

            working_hour = berth.loc[idx, "work"]

            if second_ship_ETB < first_ship_ETD + timedelta(hours=2):

                # 한 선석에서 두 척 접안할 수 있는 경우
                # 한 선석의 최대 이용 가능 비트는 25이다.
                # 한 선석에 연쇄적으로 접안할 선박 2개를 비교하여 (차지 bitt 수) 25 이하인 선박들을 한 선석에 2개 넣는다.

                if first_ship_bitt + second_ship_bitt <= 25:
                    new_second_ship_ETB = first_ship_ETB
                    new_second_ship_ETD = new_second_ship_ETB + working_hour

                    berth.loc[idx, "ETB"] = new_second_ship_ETB
                    berth.loc[idx, "ETD"] = new_second_ship_ETD

                    # 한 선석에 붙을 선박 2척 중 나중에 오는 선박이 먼저 접안 할 선박보다 빨리 출항(ETD가 빠른)하는 경우
                    # 두 선박의 데이터프레임상의 위치 바꾸기
                    if new_second_ship_ETD < first_ship_ETD:
                        temp = berth.loc[idx - 1].copy()
                        berth.loc[idx - 1] = berth.loc[idx]
                        berth.loc[idx] = temp
                    # 2척 접안하는 선박의 의미인 dupl이 1로 변경
                    berth.loc[idx, "dupl"] = 1

                # 한 선석에 1개의 선박이 접안하는 경우
                else:
                    new_second_ship_ETB = first_ship_ETD + timedelta(hours=2)
                    new_second_ship_ETD = new_second_ship_ETB + working_hour

                    berth.loc[idx, "ETB"] = new_second_ship_ETB
                    berth.loc[idx, "ETD"] = new_second_ship_ETD
    initplan_f = pd.concat([t1, t2, t3], ignore_index=True)
    initplan_f = initplan_f.sort_values(by='ETB')
    initplan_f = initplan_f.reset_index(drop=True)
    tos_plan = initplan_f.loc[:,
               ['VOYAGE', 'BERTH_NO', 'ALONG_SIDE', 'LOAD_QTY', 'DIS_QTY', 'work', 'bitt', 'ETA', 'ETB', 'ETD',
                'VSL_NM', 'LOA', 'WIDTH', 'dupl']]
    cntr = tos_plan.loc[:, ['VOYAGE', 'BERTH_NO']]
    ot = ot.drop_duplicates()
    drop_voyage_list = ot["VOYAGE"].values
    drop_idx_list = []
    for voyage in drop_voyage_list:
        drop_idx = ais_data_accumulated.query(f"VOYAGE == '{voyage}'").index
        try:
            drop_idx_list.append(drop_idx[0])
        except:
            pass
    for idx in tos_plan.index:
        voyage = tos_plan.loc[idx, "VOYAGE"]
        vsl_nm = plan_data.query(f"VOYAGE == '{voyage}'")["VSL_NM"].values[0]
        tos_plan.loc[idx, "VSL_NM"] = vsl_nm
    # 부산이 목적지인 선박들 정리
    idx_list = []
    for idx in ais_data_accumulated.index:
        DESTINATION = ais_data_accumulated.loc[idx, "DESTINATION"]
        try:
            if "BUS" in DESTINATION:
                idx_list.append(idx)
            elif "PUS" in DESTINATION:
                idx_list.append(idx)

        except:
            pass
    ais_busan_arriving = ais_data_accumulated.loc[idx_list, :]
    df = ais_busan_arriving.loc[ais_busan_arriving.query('SPEED > 2').index, :]
    voyage_list = df["VOYAGE"].values
    for idx in tos_plan.index:
        voyage = tos_plan.loc[idx, "VOYAGE"]
        if voyage in voyage_list:
            ais_df = ais_data_accumulated.query(f"VOYAGE == '{voyage}'")
            index = ais_df.index[0]
            ais_eta = ais_df['PREDICTED_ETA'][index]
            tos_plan.loc[idx, 'ais_eta'] = ais_eta
            tos_plan.loc[idx, 'ais_etb'] = ais_eta + timedelta(hours=1)
            tos_plan.loc[idx, 'ais_etd'] = ais_eta + tos_plan.loc[
                idx, "work"]  # TODO : 선박의 작업시간이 계산되면 연결하기
        else:
            ais_eta = tos_plan.loc[idx, 'ETA']
            tos_plan.loc[idx, 'ais_eta'] = ais_eta
            tos_plan.loc[idx, 'ais_etb'] = ais_eta + timedelta(hours=1)
            tos_plan.loc[idx, 'ais_etd'] = ais_eta + tos_plan.loc[
                idx, "work"]  # TODO : 선박의 작업시간이 계산되면 연결하기
    sample = pd.DataFrame(columns=[
        'VOYAGE',
        'BERTH_NO',
        'work',
        'bitt',
        'ETA',
        'ETB',
        'ETD',
        'ais_eta',
        'ais_etb',
        'ais_etd',
    ])
    t1_new = pd.DataFrame(columns=[
        'VOYAGE', 'BERTH_NO', 'FROM_BITT', 'TO_BITT', 'ETA', 'ETB', 'ETD', 'bitt',
        'work', 'VSL_NM', 'LOA'
    ])
    t2_new = pd.DataFrame(columns=[
        'VOYAGE', 'BERTH_NO', 'FROM_BITT', 'TO_BITT', 'ETA', 'ETB', 'ETD', 'bitt',
        'work', 'VSL_NM', 'LOA'
    ])
    t3_new = pd.DataFrame(columns=[
        'VOYAGE', 'BERTH_NO', 'FROM_BITT', 'TO_BITT', 'ETA', 'ETB', 'ETD', 'bitt',
        'work', 'VSL_NM', 'LOA'
    ])
    t1_ais = tos_plan[tos_plan['BERTH_NO'] == 'T1']
    t2_ais = tos_plan[tos_plan['BERTH_NO'] == 'T2']
    t3_ais = tos_plan[tos_plan['BERTH_NO'] == 'T3']
    # ot_ais = plan_data[plan_data['BERTH_NO'] == 'OT']
    t1_ais = t1_ais.drop_duplicates(['VOYAGE'], keep='first')
    t2_ais = t2_ais.drop_duplicates(['VOYAGE'], keep='first')
    t3_ais = t3_ais.drop_duplicates(['VOYAGE'], keep='first')
    t1_ais = t1_ais.reset_index(drop=True)
    t2_ais = t2_ais.reset_index(drop=True)
    t3_ais = t3_ais.reset_index(drop=True)
    for aa, row in t1_ais.iterrows():
        if aa != len(t1_ais) - 1:
            if t1_ais.ais_etd[aa] > t1_ais.ETB[aa + 1] - timedelta(hours=2):
                ex = t1_ais.loc[[aa]]
                sample = sample.append(ex, ignore_index=True)
            else:
                if t1_ais.ais_etb[aa] <= t1_ais.ETB[aa]:
                    t1_ais.ETB[aa] = t1_ais.ETB[aa]
                    t1_ais.ETD[aa] = t1_ais.ETD[aa]
                else:
                    t1_ais.ETB[aa] = t1_ais.ais_etb[aa]
                    t1_ais.ETD[aa] = t1_ais.ais_etd[aa]
                    # t1_ais.ETD[aa]=t1_ais.ETB[aa+1]-timedelta(hours=2)
                    # t1_ais.ETB[aa]=t1_ais.ETD[aa]-t1_ais.work[aa]
                new = t1_ais.loc[[aa]]
                t1_new = t1_new.append(new, ignore_index=True)
        else:
            if t1_ais.ais_etd[aa] > t1_ais.ETD[aa]:
                ex = t1_ais.loc[[aa]]
                sample = sample.append(ex, ignore_index=True)
            else:
                if t1_ais.ais_etb[aa] <= t1_ais.ETB[aa]:
                    t1_ais.ETB[aa] = t1_ais.ETB[aa]
                    t1_ais.ETD[aa] = t1_ais.ETD[aa]
                else:
                    t1_ais.ETB[aa] = t1_ais.ais_etb[aa]
                    t1_ais.ETD[aa] = t1_ais.ais_etd[aa]
                    # t1_ais.ETD[aa]=t1_ais.ETB[aa+1]-timedelta(hours=2)
                    # t1_ais.ETB[aa]=t1_ais.ETD[aa]-t1_ais.work[aa]
                new = t1_ais.loc[[aa]]
                t1_new = t1_new.append(new, ignore_index=True)
    for bb, row in t2_ais.iterrows():
        if bb != len(t2_ais) - 1:
            if t2_ais.ais_etd[bb] > t2_ais.ETB[bb + 1] - timedelta(hours=2):
                ex = t2_ais.loc[[bb]]
                sample = sample.append(ex, ignore_index=True)
            else:
                if t2_ais.ais_etb[bb] <= t2_ais.ETB[bb]:
                    t2_ais.ETB[bb] = t2_ais.ETB[bb]
                    t2_ais.ETD[bb] = t2_ais.ETD[bb]
                else:
                    t2_ais.ETB[bb] = t2_ais.ais_etb[bb]
                    t2_ais.ETD[bb] = t2_ais.ais_etd[bb]
                    # t1_ais.ETD[aa]=t1_ais.ETB[aa+1]-timedelta(hours=2)
                    # t1_ais.ETB[aa]=t1_ais.ETD[aa]-t1_ais.work[aa]
                new = t2_ais.loc[[bb]]
                t2_new = t2_new.append(new, ignore_index=True)
        else:
            if t2_ais.ais_etd[bb] > t2_ais.ETD[bb]:
                ex = t2_ais.loc[[bb]]
                sample = sample.append(ex, ignore_index=True)
            else:
                if t2_ais.ais_etb[bb] <= t2_ais.ETB[bb]:
                    t2_ais.ETB[bb] = t2_ais.ETB[bb]
                    t2_ais.ETD[bb] = t2_ais.ETD[bb]
                else:
                    t2_ais.ETB[bb] = t2_ais.ais_etb[bb]
                    t2_ais.ETD[bb] = t2_ais.ais_etd[bb]
                new = t2_ais.loc[[bb]]
                t2_new = t2_new.append(new, ignore_index=True)
    for cc, row in t3_ais.iterrows():
        if cc != len(t3_ais) - 1:  # and cc != 0:
            if t3_ais.ais_etd[cc] > t3_ais.ETB[cc + 1] - timedelta(hours=2):
                ex = t3_ais.loc[[cc]]
                sample = sample.append(ex, ignore_index=True)
            else:
                if t3_ais.ais_etb[cc] <= t3_ais.ETB[cc]:
                    t3_ais.ETB[cc] = t3_ais.ETB[cc]
                    t3_ais.ETD[cc] = t3_ais.ETD[cc]
                else:
                    t3_ais.ETB[cc] = t3_ais.ais_etb[cc]
                    t3_ais.ETD[cc] = t3_ais.ais_etd[cc]
                    # t1_ais.ETD[aa]=t1_ais.ETB[aa+1]-timedelta(hours=2)
                    # t1_ais.ETB[aa]=t1_ais.ETD[aa]-t1_ais.work[aa]
                new = t3_ais.loc[[cc]]
                t3_new = t3_new.append(new, ignore_index=True)
        else:
            if t3_ais.ais_etd[cc] > t3_ais.ETD[cc]:
                ex = t3_ais.loc[[cc]]
                sample = sample.append(ex, ignore_index=True)
            else:
                if t3_ais.ais_etb[cc] <= t3_ais.ETB[cc]:
                    t3_ais.ETB[cc] = t3_ais.ETB[cc]
                    t3_ais.ETD[cc] = t3_ais.ETD[cc]
                else:
                    t3_ais.ETB[cc] = t3_ais.ais_etb[cc]
                    t3_ais.ETD[cc] = t3_ais.ais_etd[cc]
                    # t1_ais.ETD[aa]=t1_ais.ETB[aa+1]-timedelta(hours=2)
                    # t1_ais.ETB[aa]=t1_ais.ETD[aa]-t1_ais.work[aa]
                new = t3_ais.loc[[cc]]
                t3_new = t3_new.append(new, ignore_index=True)
    t1_blank_dict = {"berth_no": [], "etb": [], "etd": [], "time": []}
    for aaa, row in t1_new.iterrows():
        if aaa != len(t1_new) - 1:
            etb = t1_new.ETD[aaa] + timedelta(hours=2)
            etd = t1_new.ETB[aaa + 1] - timedelta(hours=2)
            diff = etd - etb

            t1_blank_dict["etb"].append(etb)
            t1_blank_dict["etd"].append(etd)
            t1_blank_dict["time"].append(diff)
            t1_blank_dict["berth_no"].append("T1")
    t2_blank_dict = {"berth_no": [], "etb": [], "etd": [], "time": []}
    for bbb, row in t2_new.iterrows():
        if bbb != len(t2_new) - 1:
            etb = t2_new.ETD[bbb] + timedelta(hours=2)
            etd = t2_new.ETB[bbb + 1] - timedelta(hours=2)
            diff = etd - etb

            t2_blank_dict["etb"].append(etb)
            t2_blank_dict["etd"].append(etd)
            t2_blank_dict["time"].append(diff)
            t2_blank_dict["berth_no"].append("T2")
    t3_blank_dict = {"berth_no": [], "etb": [], "etd": [], "time": []}
    for ccc, row in t3_new.iterrows():
        if ccc != len(t3_new) - 1:
            etb = t3_new.ETD[ccc] + timedelta(hours=2)
            etd = t3_new.ETB[ccc + 1] - timedelta(hours=2)
            diff = etd - etb

            t3_blank_dict["etb"].append(etb)
            t3_blank_dict["etd"].append(etd)
            t3_blank_dict["time"].append(diff)
            t3_blank_dict["berth_no"].append("T3")
    t1_blank = pd.DataFrame(t1_blank_dict)
    t2_blank = pd.DataFrame(t2_blank_dict)
    t3_blank = pd.DataFrame(t3_blank_dict)
    blank = pd.concat([t1_blank, t2_blank, t3_blank], ignore_index=True)
    blank = blank.sort_values(by='etb')
    blank = blank.reset_index(drop=True)
    blank['state'] = 0
    # sample.BERTH_NO을 cntr.BERTH_NO으로 변경
    for sc, row in sample.iterrows():
        best = cntr[cntr['VOYAGE'] == sample.VOYAGE[sc]]
        best = best.reset_index(drop=True)
        sample.BERTH_NO[sc] = best.BERTH_NO[0]
    one = sample[sample['VOYAGE'] == 'T1']
    one['weight1'] = 1000
    one['weight2'] = 2000
    one['weight3'] = 3000
    two = sample[sample['VOYAGE'] == 'T2']
    two['weight1'] = 2000
    two['weight2'] = 1000
    two['weight3'] = 2000
    thr = sample[sample['VOYAGE'] == 'T3']
    thr['weight1'] = 3000
    thr['weight2'] = 2000
    thr['weight3'] = 1000
    sample = pd.concat([one, two, thr], ignore_index=True)
    sample = sample.sort_values(by='ais_etb')
    sample = sample.reset_index(drop=True)
    sample['t1_wait'] = 1
    sample['t1_ter'] = 1
    sample['total1'] = 1
    sample['t2_wait'] = 1
    sample['t2_ter'] = 1
    sample['total2'] = 1
    sample['t3_wait'] = 1
    sample['t3_ter'] = 1
    sample['total3'] = 1
    # sample 선박의 work와 blank의 time 비교
    # if sample.ais_eta >= blank.time일 때
    # if blank.time > sample.work 이면
    # t1_new, t2_new, t3_new를 찾아
    # new.ETB - timedelta(hours=2) == blank.etd인 new에 선박 배치
    for idx in sample.index:
        pos1 = blank[blank['etb'] >= sample.ais_eta[idx]]
        pos2 = pos1[pos1['time'] >= sample.work[idx]]
        pos3 = pos2[pos2['state'] == 0]
        if pos3.empty != True:  # 선석에 자리가 있으면 바로 배치
            pos = pos3.iloc[0]
            for idx2 in blank.index:
                if blank.etb[idx2] == pos['etb']:
                    if blank.berth_no[idx2] == pos['berth_no']:
                        blank.state[idx2] = 1
                        sample.BERTH_NO[idx] = blank.berth_no[idx2]
                        sample.ETB[idx] = blank.etb[idx2]
                        sample.ETD[idx] = sample.ETB[idx] + sample.work[idx]
                        break
            berth_no = pos["berth_no"].values[0]
            if berth_no == "T1":
                for t1_idx in t1_new.index:
                    t1_etb = t1_new.loc[t1_idx, "ETB"]
                    if pos["etd"] == t1_etb - timedelta(hours=2):
                        sample.loc[idx - 1] = sample.loc[idx]
                        sample.drop(idx, inplace=True)
            elif berth_no == "T2":
                for t2_idx in t2_new.index:
                    t2_etb = t2_new.loc[t1_idx, "ETB"]
                    if pos["etd"] == t2_etb - timedelta(hours=2):
                        sample.loc[idx - 1] = sample.loc[idx]
                        sample.drop(idx, inplace=True)
            elif berth_no == "T3":
                for t3_idx in t3_new.index:
                    t3_etb = t3_new.loc[t1_idx, "ETB"]
                    if pos["etd"] == t3_etb - timedelta(hours=2):
                        sample.loc[idx - 1] = sample.loc[idx]
                        sample.drop(idx, inplace=True)

            # if pos['berth_no'].values[0] = 'T1':

            # pso.berth_no이 T1이면
        #         t1_new 중 pos['etd'] == t1_new.ETB[z]-timedelta(hours=2)인 인덱스 앞 행에
        # sample[s] 행 추가
        # sample에서 s행 제거
        # T2면 t2_new에, T3면 t3_new에 추가

        else:  # 선석에 자리가 없으면
            # 1선석일 때
            front1 = t1_new.tail(n=1)
            t1_time = front1['ETD'] + timedelta(hours=1) - sample.ais_eta[idx]
            sample.t1_wait[idx] = t1_time * 1000
            sample.t1_ter[idx] = sample.weight1[idx]
            sample.total1[idx] = sample.t1_wait[idx] + sample.t1_ter[idx]
            # 2선석일 때
            front2 = t2_new.tail(n=1)
            t2_time = front2['ETD'] + timedelta(hours=1) - sample.ais_eta[idx]
            sample.t2_wait[idx] = t2_time * 1000
            sample.t2_ter[idx] = sample.weight2[idx]
            sample.total2[idx] = sample.t2_wait[idx] + sample.t2_ter[idx]
            # 3선석일 때
            front3 = t3_new.tail(n=1)
            t3_time = front3['ETD'] + timedelta(hours=1) - sample.ais_eta[idx]
            sample.t3_wait[idx] = t3_time * 1000
            sample.t3_ter[idx] = sample.weight3[idx]
            sample.total3[idx] = sample.t3_wait[idx] + sample.t3_ter[idx]
            total1 = sample.total1[idx]
            total2 = sample.total2[idx]
            total3 = sample.total3[idx]

            costlist = [total1, total2, total3]
            cost = costlist.index(min(costlist))
            if cost + 1 == 1:
                sample.ETB[idx] = front1['ETD'] + timedelta(hours=2)
                sample.ETD[idx] = sample.ETB[idx] + sample.work[idx]
                allo = sample.loc[[idx]]
                t1_new = t1_new.append(allo, ignore_index=True)
            elif cost + 1 == 2:
                sample.ETB[idx] = front2['ETD'] + timedelta(hours=2)
                sample.ETD[idx] = sample.ETB[idx] + sample.work[idx]
                allo = sample.loc[[idx]]
                t2_new = t2_new.append(allo, ignore_index=True)
            else:
                sample.ETB[idx] = front3['ETD'] + timedelta(hours=2)
                sample.ETD[idx] = sample.ETB[idx] + sample.work[idx]
                allo = sample.loc[[idx]]
                t3_new = t3_new.append(allo, ignore_index=True)

    cplan = pd.concat([t1_new, t2_new, t3_new], ignore_index=True)
    cplan = cplan.sort_values(by='ETB')
    cplan = cplan.reset_index(drop=True)
    t1_new['real_FROM_BITT'] = 0
    t1_new['real_TO_BITT'] = 0
    t2_new['real_FROM_BITT'] = 0
    t2_new['real_TO_BITT'] = 0
    t3_new['real_FROM_BITT'] = 0
    t3_new['real_TO_BITT'] = 0
    t1_final = t1_new
    t2_final = t2_new
    t3_final = t3_new
    for q1, row in t1_final.iterrows():
        if t1_final.dupl[q1] == 0:
            t1_final.real_FROM_BITT[q1] = 1
            t1_final.real_TO_BITT[q1] = t1_final.real_FROM_BITT[q1] + abs(
                t1_final.bitt[q1])
        else:
            t1_final.real_FROM_BITT[q1] = t1_final.real_TO_BITT[q1 - 1] - 1
            t1_final.real_TO_BITT[q1] = t1_final.real_FROM_BITT[q1] + abs(
                t1_final.bitt[q1])

    for w1, row in t2_final.iterrows():
        b1 = t1_final[t1_final['ETB'] <= t2_final.ETD[w1]]
        b1 = b1[b1['ETD'] >= t2_final.ETB[w1]]
        # b1 = b1.max()['real_TO_BITT']
        # b1 = b1.tail(n=1)
        # b1 = b1.reset_index(drop=True)
        # display(b1)
        if b1.empty == True:
            if t2_final.dupl[w1] == 0:
                t2_final.real_FROM_BITT[w1] = 23
                t2_final.real_TO_BITT[w1] = t2_final.real_FROM_BITT[w1] + abs(
                    t2_final.bitt[w1])
            else:
                t2_final.real_FROM_BITT[w1] = t2_final.real_TO_BITT[w1 - 1] - 1
                t2_final.real_TO_BITT[w1] = t2_final.real_FROM_BITT[w1] + abs(
                    t2_final.bitt[w1])

        else:
            if t2_final.dupl[w1] == 0:
                t2_final.real_FROM_BITT[w1] = b1.max()['real_TO_BITT'] - 1
                t2_final.real_TO_BITT[w1] = t2_final.real_FROM_BITT[w1] + abs(
                    t2_final.bitt[w1])
            else:
                t2_final.real_FROM_BITT[w1] = t2_final.real_TO_BITT[w1 - 1] - 1
                t2_final.real_TO_BITT[w1] = t2_final.real_FROM_BITT[w1] + abs(
                    t2_final.bitt[w1])

    for e1, row in t3_final.iterrows():
        b2 = t2_final[t2_final['ETB'] <= t3_final.ETD[e1]]
        b2 = b2[b2['ETD'] >= t3_final.ETB[e1]]
        # b2 = b2.tail(n=1)
        # b2 = b2.reset_index(drop=True)

        if b2.empty == True:
            if t3_final.dupl[e1] == 0:
                t3_final.real_TO_BITT[e1] = 63
                t3_final.real_FROM_BITT[e1] = t3_final.real_TO_BITT[e1] - abs(
                    t3_final.bitt[e1])
            else:
                t3_final.real_TO_BITT[e1] = t3_final.real_FROM_BITT[e1 - 1] + 1
                t3_final.real_FROM_BITT[e1] = t3_final.real_TO_BITT[e1] - abs(
                    t3_final.bitt[e1])

        else:
            if t3_final.dupl[e1] == 0:
                t3_final.real_FROM_BITT[e1] = b2.max()['real_TO_BITT'] - 1
                t3_final.real_TO_BITT[e1] = t3_final.real_FROM_BITT[e1] + abs(
                    t3_final.bitt[e1])
            else:
                t3_final.real_FROM_BITT[e1] = t3_final.real_TO_BITT[e1 - 1] - 1
                t3_final.real_TO_BITT[e1] = t3_final.real_FROM_BITT[e1] + abs(
                    t3_final.bitt[e1])

    plan_final = pd.concat([t1_final, t2_final, t3_final], ignore_index=True)
    plan_final = plan_final.sort_values(by='ETB')
    plan_final = plan_final.reset_index(drop=True)
    plan_final['FROM_BITT'] = plan_final['real_FROM_BITT'] + 1
    plan_final['TO_BITT'] = plan_final['real_TO_BITT'] - 1
    for r, row in plan_final.iterrows():
        if plan_final.ALONG_SIDE[r] != 'P':
            bittnum = plan_final.real_TO_BITT[q1].copy()
            plan_final.real_TO_BITT[q1] = plan_final.real_FROM_BITT[q1]
            plan_final.real_FROM_BITT[q1] = bittnum
            viewnum = plan_final.TO_BITT[q1].copy()
            plan_final.TO_BITT[q1] = plan_final.FROM_BITT[q1]
            plan_final.FROM_BITT[q1] = bittnum
    plan_final = plan_final.loc[:, [
                                       'VOYAGE', 'VSL_NM', 'BERTH_NO', 'FROM_BITT', 'TO_BITT', 'ETB', 'ETD',
                                       'ALONG_SIDE', 'DIS_QTY', 'LOAD_QTY', 'LOA', 'WIDTH'
                                   ]]
    add = tos_plan_berth_df.loc[:, [
                                       'VOYAGE', 'VSL_CD', 'CALL_YEAR', 'CALL_SEQ', 'PTNR_CODE', 'timestamp'
                                   ]]
    final = pd.merge(plan_final, add, on='VOYAGE', how='left')
    final = final.reindex([
        'VSL_CD', 'VOYAGE', 'CALL_YEAR', 'CALL_SEQ', 'VSL_NM', 'PTNR_CODE',
        'BERTH_NO', 'FROM_BITT', 'TO_BITT', 'ETB', 'ETD', 'ALONG_SIDE', 'DIS_QTY',
        'LOAD_QTY', 'LOA', 'WIDTH', 'timestamp'
    ],
        axis=1)
    final.columns = [
        'VSL_CD', 'VOYAGE', 'YR', 'SER_NO', 'VSL_NM', 'PTNR_CODE', 'BERTH_C',
        'FROM_BITT_C', 'TO_BITT_C', 'PTA', 'PTD', 'ALONG_SIDE', 'DIS_QTY',
        'LOAD_QTY', 'LOA', 'WIDTH', 'TIMESTAMP'
    ]
    final.rename(columns={"TIMESTAMP": 'timestamp'},
                 inplace=True)
    final["timestamp"] = final["timestamp"].apply(
        lambda x: pd.to_datetime(str(x)[:-7]))

    final["c_id"] = 'seq_berth_c.NEXTVAL'

    return final


def tug_allocation(tuglist, mutual_optimal_plan_berth_c_df, tos_vessel):
    tuglist['GRADE'] = 0
    tuglist['STATE'] = 0
    for ii, row in tuglist.iterrows():
        if tuglist.at[ii, 'HORSEPOWER'] < 2000:
            tuglist.at[ii, 'GRADE'] = 1
        elif 2000 <= tuglist.at[ii, 'HORSEPOWER'] < 3000:
            tuglist.at[ii, 'GRADE'] = 2
        elif 3000 <= tuglist.at[ii, 'HORSEPOWER'] < 4000:
            tuglist.at[ii, 'GRADE'] = 3
        elif 4000 <= tuglist.at[ii, 'HORSEPOWER'] < 5000:
            tuglist.at[ii, 'GRADE'] = 4
        elif 5000 <= tuglist.at[ii, 'HORSEPOWER'] < 6000:
            tuglist.at[ii, 'GRADE'] = 5
        else:
            tuglist.at[ii, 'GRADE'] = 6
    tug1 = tuglist[tuglist['GRADE'] == 1]
    tug2 = tuglist[tuglist['GRADE'] == 2]
    tug3 = tuglist[tuglist['GRADE'] == 3]
    tug4 = tuglist[tuglist['GRADE'] == 4]
    tug5 = tuglist[tuglist['GRADE'] == 5]
    tug6 = tuglist[tuglist['GRADE'] == 6]
    tug1.reset_index(inplace=True, drop=True)
    tug2.reset_index(inplace=True, drop=True)
    tug3.reset_index(inplace=True, drop=True)
    tug4.reset_index(inplace=True, drop=True)
    tug5.reset_index(inplace=True, drop=True)
    tug6.reset_index(inplace=True, drop=True)
    berthplan = mutual_optimal_plan_berth_c_df
    berthplan = berthplan.loc[:, ['VSL_CD', 'VOYAGE', 'VSL_NM', 'BERTH_C', 'PTA', 'PTD']]
    data = tos_vessel.loc[:, ['VSL_CD', 'CALL_SIGN', 'LOA']]
    berthplan = pd.merge(berthplan, data, on='VSL_CD', how='left')
    berthplan.drop_duplicates(inplace=True)
    berthplan.reset_index(inplace=True, drop=True)
    berthplan['GRTG'] = 0
    berthplan['GRTG'] = 0.0038 * (pow(berthplan['LOA'], 2.9579))
    berthplan['tug1num'] = 0
    berthplan['tug2num'] = 0
    berthplan['tug3num'] = 0
    berthplan['tug4num'] = 0
    berthplan['tug5num'] = 0
    berthplan['tug6num'] = 0
    for jj, row in berthplan.iterrows():

        if berthplan.at[jj, 'GRTG'] < 5000:
            berthplan.at[jj, 'tug1num'] = 1
        elif berthplan.at[jj, 'GRTG'] < 10000:
            berthplan.at[jj, 'tug1num'] = 1
            berthplan.at[jj, 'tug2num'] = 1
        elif berthplan.at[jj, 'GRTG'] < 20000:
            berthplan.at[jj, 'tug2num'] = 2
        elif berthplan.at[jj, 'GRTG'] < 40000:
            berthplan.at[jj, 'tug2num'] = 1
            berthplan.at[jj, 'tug3num'] = 1
        elif berthplan.at[jj, 'GRTG'] < 70000:
            berthplan.at[jj, 'tug3num'] = 1
            berthplan.at[jj, 'tug4num'] = 1
        elif berthplan.at[jj, 'GRTG'] < 100000:
            berthplan.at[jj, 'tug3num'] = 2
            berthplan.at[jj, 'tug5num'] = 1
        elif berthplan.at[jj, 'GRTG'] < 150000:
            berthplan.at[jj, 'tug4num'] = 2
            berthplan.at[jj, 'tug5num'] = 1
        else:
            berthplan.at[jj, 'tug4num'] = 2
            berthplan.at[jj, 'tug6num'] = 1
    tug_allo = berthplan.loc[:, ['VOYAGE', 'tug1num', 'tug2num', 'tug3num', 'tug4num', 'tug5num', 'tug6num']]
    vessel_eta = berthplan.loc[:, ['CALL_SIGN', 'VSL_NM', 'VOYAGE', 'BERTH_C', 'PTA']]
    vessel_eta['et'] = 4
    vessel_eta['PTA'] = vessel_eta['PTA'] - timedelta(hours=1)
    vessel_etd = berthplan.loc[:, ['CALL_SIGN', 'VSL_NM', 'VOYAGE', 'BERTH_C', 'PTD']]
    vessel_etd['et'] = 5
    vessel_eta.columns = ['CALL_SIGN', 'VSL_NM', 'VOYAGE', 'BERTH', 'time', 'et']
    vessel_etd.columns = ['CALL_SIGN', 'VSL_NM', 'VOYAGE', 'BERTH', 'time', 'et']
    new_tbl = pd.concat([vessel_eta, vessel_etd])
    new_tbl = new_tbl.sort_values(by="time")
    new_tbl.reset_index(inplace=True, drop=True)
    new_tbl.rename(columns={"time": "TUG_S"}, inplace=True)
    total_tbl = new_tbl
    total_tbl['TUG_F'] = total_tbl['TUG_S'] + timedelta(hours=1)
    total_tbl['TUG1'] = 'None'
    total_tbl['TUG2'] = 'None'
    total_tbl['TUG3'] = 'None'
    total_tbl['cost'] = 0
    tug_allo = pd.merge(tug_allo, total_tbl, on='VOYAGE', how='left')
    a = 0
    b = 0
    c = 0
    d = 0
    e = 0
    f = 0
    for kk in range(len(tug_allo)):
        if tug_allo.tug1num[kk] != 0:
            www = tug_allo.tug1num[kk]
            for rr in range(www):
                if a == len(tug1):
                    a = 0
                if tug_allo.TUG1[kk] == 'None':
                    tug_allo.TUG1[kk] = tug1.NAME[a]
                    a = a + 1
                elif tug_allo.TUG2[kk] == 'None':
                    tug_allo.TUG2[kk] = tug1.NAME[a]
                    a = a + 1
                else:
                    tug_allo.TUG3[kk] = tug1.NAME[a]
                    a = a + 1
        else:
            pass

        if tug_allo.tug2num[kk] != 0:
            www = tug_allo.tug2num[kk]
            for rr in range(www):
                if b == len(tug2):
                    b = 0
                if tug_allo.TUG1[kk] == 'None':
                    tug_allo.TUG1[kk] = tug2.NAME[b]
                    b = b + 1
                elif tug_allo.TUG2[kk] == 'None':
                    tug_allo.TUG2[kk] = tug2.NAME[b]
                    b = b + 1
                else:
                    tug_allo.TUG3[kk] = tug2.NAME[b]
                    b = b + 1
        else:
            pass

        if tug_allo.tug3num[kk] != 0:
            www = tug_allo.tug3num[kk]
            for rr in range(www):
                if c == len(tug3):
                    c = 0
                if tug_allo.TUG1[kk] == 'None':
                    tug_allo.TUG1[kk] = tug3.NAME[c]
                    c = c + 1
                elif tug_allo.TUG2[kk] == 'None':
                    tug_allo.TUG2[kk] = tug3.NAME[c]
                    c = c + 1
                else:
                    tug_allo.TUG3[kk] = tug3.NAME[c]
                    c = c + 1
        else:
            pass

        if tug_allo.tug4num[kk] != 0:
            www = tug_allo.tug4num[kk]
            for rr in range(www):
                if d == len(tug4):
                    d = 0
                if tug_allo.TUG1[kk] == 'None':
                    tug_allo.TUG1[kk] = tug4.NAME[d]
                    d = d + 1
                elif tug_allo.TUG2[kk] == 'None':
                    tug_allo.TUG2[kk] = tug4.NAME[d]
                    d = d + 1
                else:
                    tug_allo.TUG3[kk] = tug4.NAME[d]
                    d = d + 1
        else:
            pass

        if tug_allo.tug5num[kk] != 0:
            www = tug_allo.tug5num[kk]
            for rr in range(www):
                if e == len(tug5):
                    e = 0
                if tug_allo.TUG1[kk] == 'None':
                    tug_allo.TUG1[kk] = tug5.NAME[e]
                    e = e + 1
                elif tug_allo.TUG2[kk] == 'None':
                    tug_allo.TUG2[kk] = tug5.NAME[e]
                    e = e + 1
                else:
                    tug_allo.TUG3[kk] = tug5.NAME[e]
                    e = e + 1
        else:
            pass

        if tug_allo.tug6num[kk] != 0:
            www = tug_allo.tug6num[kk]
            for rr in range(www):
                if f == len(tug6):
                    f = 0
                if tug_allo.TUG1[kk] == 'None':
                    tug_allo.TUG1[kk] = tug6.NAME[f]
                    f = f + 1
                elif tug_allo.TUG2[kk] == 'None':
                    tug_allocation.TUG2[kk] = tug6.NAME[f]
                    f = f + 1
                else:
                    tug_allo.TUG3[kk] = tug6.NAME[f]
                    f = f + 1
        else:
            pass
    for tt in range(len(tug_allo)):
        for mm in range(11, 14):

            for abc in range(len(tuglist)):
                if tug_allo.iloc[tt, mm] == tuglist.NAME[abc]:
                    if tuglist.HORSEPOWER[abc] <= 500:
                        tug_allo.cost[tt] = tug_allo.cost[tt] + 205890
                    elif 500 < tuglist.HORSEPOWER[abc] <= 1000:
                        tug_allo.cost[tt] = tug_allo.cost[tt] + 318840
                    elif 1000 < tuglist.HORSEPOWER[abc] <= 1500:
                        tug_allo.cost[tt] = tug_allo.cost[tt] + 446960
                    elif 1500 < tuglist.HORSEPOWER[abc] <= 2000:
                        tug_allo.cost[tt] = tug_allo.cost[tt] + 577990
                    elif 2000 < tuglist.HORSEPOWER[abc] <= 2500:
                        tug_allo.cost[tt] = tug_allo.cost[tt] + 692010
                    elif 2500 < tuglist.HORSEPOWER[abc] <= 3000:
                        tug_allo.cost[tt] = tug_allo.cost[tt] + 794560
                    elif 3000 < tuglist.HORSEPOWER[abc] <= 3500:
                        tug_allo.cost[tt] = tug_allo.cost[tt] + 896500
                    elif 3500 < tuglist.HORSEPOWER[abc] <= 4000:
                        tug_allo.cost[tt] = tug_allo.cost[tt] + 577990
                    elif 4000 < tuglist.HORSEPOWER[abc] <= 4500:
                        tug_allo.cost[tt] = tug_allo.cost[tt] + 1098630
                    elif 4500 < tuglist.HORSEPOWER[abc] <= 5000:
                        tug_allo.cost[tt] = tug_allo.cost[tt] + 1198620
                    elif 5000 < tuglist.HORSEPOWER[abc] <= 5500:
                        tug_allo.cost[tt] = tug_allo.cost[tt] + 1296890
                    elif 5500 < tuglist.HORSEPOWER[abc] <= 6000:
                        tug_allo.cost[tt] = tug_allo.cost[tt] + 1393550
                    else:
                        exceed = tuglist.HORSEPOWER[abc] - 6000
                        tug_allo.cost[tt] = 1393550 + (exceed / 100 * 19332)
                    break
            if tug_allo.TUG2[tt] == 'None':
                break
            else:
                if mm == 12:
                    if tug_allo.TUG3[tt] == 'None':
                        break
                continue
    tug_final = tug_allo.loc[:,
                ['CALL_SIGN', 'VSL_NM', 'VOYAGE', 'BERTH', 'TUG_S', 'TUG_F', 'et', 'TUG1', 'TUG2', 'TUG3', 'cost']]

    tug_allocation_df = tug_final
    tug_allocation_df['TA_ID'] = 'seq_tug_allo.NEXTVAL'
    tug_allocation_df.rename(columns={
        "VSL_NM": "VSSL_ENG_NM",
        "TUG_S": "TUG_START_TIME",
        "TUG_F": "TUG_FIN_DATE",
        "TUG1": "TUG1_NAME",
        "TUG2": "TUG2_NAME",
        "TUG3": "TUG3_NAME",
        "CALL_SIGN": "CLSGN",

    },
        inplace=True)
    return tug_allocation_df
