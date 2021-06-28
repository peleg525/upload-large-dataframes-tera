import pandas as pd
import teradatasql as t
import math


def upload_how_many_rows_we_want_main(df, q, table_name):
    """
    uploading large dataframes to tera
    
    ARGS:
    df - dataframe
    q - string - query
    table_name - string
    
    example format:
    q = Insert into d_digital_data.blabla values (?, ?, ?, ?)
    table_name = d_digital_data.blabla
    
    Return:
    nan
    """
    l = uploade_how_many_rows_we_want(df, q, table_name, l=[])
    if len(l)==0:
        print("done")
    else:
        # union all tables
        con = t.connect('{"host":"tdprd","logmech":"krb5"}')
        cur = con.cursor ()
        q_union = "sel * from {0}".format(l[0])
        for item in l[1:]:
            q_union +=" union all sel * from {0}".format(item)

        table_name_new = table_name[:table_name.find("tmp_tmp")]

        q_final = """insert into {0}
        {1}
        """.format(table_name, q_union)
        cur.execute(q_final)
        for item in l:
            cur.execute("drop table {0}".format(item))
        print('done')

def uploade_how_many_rows_we_want(df, q, table_name, l=[]):
    """
    A recursion that will divide our data into several parts and upload them to tera.
    
    ARGS:
    df - dataframe
    q - string - query
    table_name - string
    l - list - keep empty
    
    example format:
    q = Insert into d_digital_data.blabla values (?, ?, ?, ?)
    table_name = d_digital_data.blabla
    
    Return:
    nan
    """
    con = t.connect('{"host":"tdprd","logmech":"krb5"}')
    cur = con.cursor ()
    print("len of rows: " + str(len(df)))
    try:
        cur.execute(q, df.values.tolist())
    except Exception as ex:
        if "batch request" in str(ex):
            con = t.connect('{"host":"tdprd","logmech":"krb5"}')
            cur = con.cursor ()
            # create new tables in tera
            create_statment = cur.execute("show table " + table_name).fetchall()[0]
            create_statment = create_statment[0].replace('\r', '\n')
            create_statment1 = create_statment.replace(table_name, table_name+'tmp_tmp1')
            create_statment2 = create_statment.replace(table_name, table_name+'tmp_tmp2')
            cur.execute(create_statment1)
            cur.execute(create_statment2)

            # usally, tera upload some of the data before crashing.
            # we dont want duplicates.
            cur.execute("delete {0}".format(table_name))

            # split the data to 2 dataframes
            len_data = math.ceil(len(df)/2)
            df1 = df.iloc[:len_data]
            df2 = df.iloc[len_data:]
            
            # replace query
            q1 = q.replace(table_name, table_name+'tmp_tmp1')
            q2 = q.replace(table_name, table_name+'tmp_tmp2')
            
            print(l)
            l1 = uploade_how_many_rows_we_want(df1, q1, table_name+'tmp_tmp1', l + [table_name+'tmp_tmp1'])
            l2 = uploade_how_many_rows_we_want(df2, q2, table_name+'tmp_tmp2', l + [table_name+'tmp_tmp2'])
            l = l+l1+l2

        else:
            print (ex)
            raise error

    return l

if __name__ == "__main__":
    upload_how_many_rows_we_want_main(df, q, table_name)