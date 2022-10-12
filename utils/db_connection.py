from .util import upper


def insert(db, table, columns, values):
    col_text = ", ".join(columns)
    val_text = ", ".join(values)
    sql = f"insert into {table} ({col_text}) values ({val_text})"

    with db.cursor() as cs:
        cs.execute(sql)
        db.commit()


def drop_and_create(db, create_sql, drop_sql, sequence_sql=None, primary_sql=None, grant_sql=None):
    cursor = db.cursor()
    try:
        cursor.execute(drop_sql)
        db.commit()
    except:
        pass
    cursor.execute(create_sql)
    try:
        cursor.execute(sequence_sql)
        db.commit()

    except:
        pass
    try:
        cursor.execute(primary_sql)
        cursor.execute(grant_sql)
    except:
        pass

    db.commit()


def dtype_list(sql_create):
    other_list = []
    varchar_list = []
    date_list = []
    table_columns = sql_create.split(' (')[1][:-1].split(',')
    for idx, val in enumerate(table_columns):
        val = val.lstrip()
        field_name, dtype = val.split(' ')[0], val.split(' ')[1]
        if dtype == "VARCHAR2(40)":
            varchar_list.append(idx)
        elif dtype == 'DATE':
            date_list.append(idx)
        else:
            other_list.append(idx)

    return varchar_list, date_list, other_list


def db_write(db, drop_sql, create_sql, seq_sql, pri_sql, grant_sql, columns, df, table_name):
    if drop_sql is not None:
        drop_and_create(db, create_sql, drop_sql, seq_sql, pri_sql, grant_sql)
    varchar_list, date_list, other_list = dtype_list(
        create_sql)
    # try:
    for idx in df.index:
        list_sample = []
        try:
            for i in range(len(columns)):
                value = df.loc[idx, columns[i]]
                list_sample.append(value)
        except:
            columns = list(map(upper, columns))
            for i in range(len(columns)):
                value = df.loc[idx, columns[i]]
                list_sample.append(value)
        list_sample = map(str, list_sample)
        value_list = []
        for i, v in enumerate(list_sample):
            if i in varchar_list:
                value_list.append("'" + v + "'")
            elif i in date_list:
                if len(v) > 19:
                    v = v[:19]
                value_list.append("TO_DATE('" + v + "', 'yyyy-mm-dd hh24:mi:ss')")

            elif i in other_list:
                value_list.append(v)
        insert(db, table_name, columns, value_list)
    # except:
    #     columns = list(map(upper, columns))
    #
    #     for idx in df.index:
    #         list_sample = []
    #         for i in range(len(columns)):
    #             value = df.loc[idx, columns[i]]
    #             list_sample.append(value)
    #         list_sample = map(str, list_sample)
    #         value_list = []
    #         for i, v in enumerate(list_sample):
    #             if i in varchar_list:
    #                 value_list.append("'" + v + "'")
    #             elif i in date_list:
    #                 if len(v) > 19:
    #                     v = v[:-7]
    #                 value_list.append("TO_DATE('" + v + "', 'yyyy-mm-dd hh24:mi:ss')")
    #             elif i in other_list:
    #                 value_list.append(v)
    #         insert(db, table_name, columns, value_list)
