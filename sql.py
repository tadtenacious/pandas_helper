import math
import numpy as np
import pandas as pd


class Tsql_Generator:
    '''A Class that is instantiated by a pandas dataframe and a sql table name.
    Currently only supports TSQL and four TSQL datatypes. All integer types are
    set to BIGINT, all float types to FLOAT, any date or datetime is set to DATE,
    and strings are set to appropriate varchar size. Currently no NULL handling.
    Usage:
    >>>import pandas as pd
    >>>from pandas_helper import Tsql_Generator
    >>>df = pd.DataFrame({
        'Column_A': ['Hello','World','My name is Tad','I hope you like', 'this module. It helps me with a lot of annoying stuff'],
        'Column_B': [1,234,4574567,67,24],
        'Column_C': [0.123424,0.12,0.99,123.324523,43563456.123],
        'Column_D': pd.date_range(start='1/1/2018', end='1/05/2018')
    })
    >>>df
        Column_A	                                        Column_B	Column_C	Column_D
    0	Hello	                                            1	        1.234240e-01	2018-01-01
    1	World	                                            234	        1.200000e-01	2018-01-02
    2	My name is Tad	                                    4574567	    9.900000e-01	2018-01-03
    3	I hope you like	                                    67	        1.233245e+02	2018-01-04
    4	this module. It helps me with a lot of annoyin...	24	        4.356346e+07	2018-01-05

    >>>tsql = Tsql_Generator(df,'my_table')
    >>>print(tsql.sql)
    CREATE TABLE my_table (
    Column_A    VARCHAR(60),
    Column_B    BIGINT,
    Column_C    FLOAT,
    Column_D    DATE
    )
    --DROP TABLE my_table

    INSERT INTO my_table (Column_A,Column_B,Column_C,Column_D) VALUES
    ('Hello',1,0.123424,'2018-01-01'),
    ('World',234,0.12,'2018-01-02'),
    ('My name is Tad',4574567,0.99,'2018-01-03'),
    ('I hope you like',67,123.324523,'2018-01-04'),
    ('this module. It helps me with a lot of annoying stuff',24,43563456.123,'2018-01-05')

    >>>tsql.save('my_table.sql')
    '''

    FORMAT_CROSSWALK_DICT = {
        'BIGINT': '{}',
        'FLOAT': '{}',
        'DATE': "'{:%Y-%m-%d}'",
        'VARCHAR': "'{}'"
    }

    def __init__(self, dataframe, table_name):
        self.dataframe = dataframe
        self.table_name = table_name
        self._check_dataframe()
        self.cross_walk_dict = self._columns_to_tsql()
        self.sql = self._generate_tsql()

    def _check_dataframe(self):
        '''Check to see if dataframe is actually a series and convert to
        dataframe is necessary.'''
        if type(self.dataframe) == pd.Series:
            self.dataframe = pd.DataFrame(self.dataframe)
        return

    def _columns_to_tsql(self):
        '''Creates a cross walk dictionary to get from pandas data types to tsql
        data types. DATA MUST BE PREPROCESSED AND CLEAN BEFORE USE! Returns a
        dictionary'''
        convert_dict = dict()
        for column in self.dataframe.columns:
            col_dtype = self.dataframe[column].dtype
            if np.issubdtype(col_dtype, np.integer):
                convert_dict[column] = 'BIGINT'
            elif np.issubdtype(col_dtype, np.floating):
                convert_dict[column] = 'FLOAT'
            elif np.issubdtype(col_dtype, np.datetime64):
                convert_dict[column] = 'DATE'
            else:
                max_len = self.dataframe[column].astype(str).apply(len).max()
                convert_dict[column] = 'VARCHAR({})'.format(
                    self._roundup_ten(max_len))
        return convert_dict

    def _generate_data_format(self):
        '''Create the format string for the tsql insert statement.'''
        flist = []
        for col in self.dataframe.columns:
            if self.cross_walk_dict[col][0:7] == 'VARCHAR':
                tsql_type = 'VARCHAR'
            else:
                tsql_type = self.cross_walk_dict[col]
            fmt = self.FORMAT_CROSSWALK_DICT[tsql_type]
            flist.append(fmt)
        data_format = '('+','.join(flist)+')'
        return data_format

    def _grouper(self, raw, split=None, fmt="'{}'", n=10, expand=False):
        '''A function to transform lists into formatted strings
        for SQL queries. Can take in a list or string.'''
        if split is None and type(raw) == list:
            if expand:
                lst = [fmt.format(*i) for i in raw]
            else:
                lst = [fmt.format(i) for i in raw]
        else:
            if expand:
                lst = [fmt.format(*i) for i in raw.split(split)]
            else:
                lst = [fmt.format(i) for i in raw.split(split)]
        out = []
        for i in range(0, len(lst), n):
            temp = ','.join(lst[i:i+n])
            out.append(temp)
        return ',\n'.join(out)

    def _roundup_ten(self, x):
        return int(math.ceil(x / 10.0)) * 10

    def _get_create_table_statement(self):
        '''Generate the TSQL create statement from a pandas dataframe. DATA MUST BE
        PREPROCESSED, CLEANED, AND COLUMNS MUST BE ORDERED BEFORE USE!'''
        begin = 'CREATE TABLE {} (\n'.format(self.table_name)
        drop = '\n--DROP TABLE {}\n'.format(self.table_name)
        create_list = []
        # spaces = roundup_ten(len(dataframe.columns.max())+10) + 2
        max_len = max(len(col_name) for col_name in self.dataframe.columns)
        spaces = self._roundup_ten(max_len) + 2
        for column in self.dataframe.columns:
            dtype = self.cross_walk_dict[column]
            if ' ' in column:
                new_col = '[' + column + ']'
                fmt = '{}{}'.format(new_col.ljust(spaces), dtype)
            else:
                fmt = '{}{}'.format(column.ljust(spaces), dtype)
            create_list.append(fmt)
        end = ',\n'.join(create_list) + '\n)'
        create_statement = begin + end + drop
        return create_statement

    def _get_insert_line(self):
        '''Generate the insert statement. Does not format data. Returns string.'''
        insert = "INSERT INTO {} ({}) VALUES"
        cols = ','.join([f"[{col}]" if ' ' in col
                         else col for col in self.dataframe.columns])
        return insert.format(self.table_name, cols)

    def _get_insert_statement(self):
        '''Generate the full TSQL Insert Statement.'''
        insert = self._get_insert_line() + '\n'
        insert_line = '\n' + insert
        data_format = self._generate_data_format()
        fdata = [data_format.format(*row) for row in self.dataframe.values]
        out = []
        for i in range(0, len(fdata), 1000):
            tmp = fdata[i:i+1000]
            grp = self._grouper(tmp, fmt="{}")
            out.append(grp)
        statement = insert + insert_line.join(out)
        return statement

    def _generate_tsql(self):
        create = self._get_create_table_statement()
        insert = self._get_insert_statement()
        full_tsql_statement = create + '\n' + insert
        return full_tsql_statement

    def save(self, file_name):
        '''Convenience function to write generated tsql to text file.'''
        with open(file_name, 'w') as f:
            f.write(self.sql)
        return
