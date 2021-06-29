import pandas as pd
import teradatasql as t
import math
import string

class upload_to_tera:
    """
    Uploading large dataframes to tera
    
    """
    
    def __init__(self, df, q, table_name):
        """
        ARGS:
        df - dataframe
        q - string - query
        table_name - string

        example format:
        q = Insert into d_digital_data.blabla values (?, ?, ?, ?)
        table_name = d_digital_data.blabla
        """
        self.df = df
        self.q = q
        self.table_name = table_name
        alphabet_string = string.ascii_lowercase
        self.alphabet_list = list(alphabet_string)+list(alphabet_string)+list(alphabet_string)+list(alphabet_string)+list(alphabet_string)
        self.upload_how_many_rows_we_want_main()
        

    def stack(self):
        return self.alphabet_list.pop()

    def upload_how_many_rows_we_want_main(self):
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
        l = self.uploade_how_many_rows_we_want(self.df, self.q, self.table_name)
        if len(l)==0:
            print("done")
        else:
            # union all tables
            con = t.connect('{"host":"tdprd","logmech":"krb5"}')
            cur = con.cursor ()
            q_union = "sel * from {0}".format(l[0])
            for item in l[1:]:
                q_union +=" union all sel * from {0}".format(item)

            q_final = """insert into {0}
            {1}
            """.format(self.table_name, q_union)
            cur.execute(q_final)
            for item in l:
                cur.execute("drop table {0}".format(item))
            print('done')

    def uploade_how_many_rows_we_want(self, df, q, table_name, l=[]):
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
                a = self.stack()
                b = self.stack()
                print(a)
                print(b)
                table_name1 = table_name + a
                table_name2 = table_name + b
                create_statment1 = create_statment.replace(table_name, table_name1)
                create_statment2 = create_statment.replace(table_name, table_name2)
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
                q1 = q.replace(table_name, table_name1)
                q2 = q.replace(table_name, table_name2)

                l1 = self.uploade_how_many_rows_we_want(df1, q1, table_name1, l + [table_name1])
                l2 = self.uploade_how_many_rows_we_want(df2, q2, table_name2, l + [table_name2])
                l = l+l1+l2

            else:
                print (ex)
                raise error

        return l