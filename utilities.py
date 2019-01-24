
import pandas as pd


def list_to_rows(dataframe, column_to_expand, sep=',', drop_index=True):
    '''
    Takes in a data frame and a column of strings with a separator. It converts
    column of lists to rows by expanding each value of the list into its own
    row and joins back to the dataframe dropping the list column. You can
    optionally set a separator.

    Possible Usage:
    >>> import pandas as pd
    >>> df = pd.DataFrame(data={'Col_A':list('ABC'),
             'Column_To_Expand':['1,2','3,4,5','6']})
    >>> df
      Col_A Column_To_Expand
    0     A              1,2
    1     B            3,4,5
    2     C                6
    >>> list_to_rows(df,'Column_To_Expand')
      Col_A Column_To_Expand
    0     A                1
    1     A                2
    2     B                3
    3     B                4
    4     B                5
    5     C                6
    >>> df = pd.DataFrame(data={'Col_A':list('ABC'),
             'Column_To_Expand':['1,2','3,4,5','6']})
    >>> df.pipe(list_to_rows,column_to_expand='Column_To_Expand')
      Col_A Column_To_Expand
    0     A                1
    1     A                2
    2     B                3
    3     B                4
    4     B                5
    5     C                6
    >>> df = pd.DataFrame(data={'Col_A':list('ABC'),
             'Column_To_Expand':['1;2','3;4;5','6']}).pipe(list_to_rows,
            column_to_expand='Column_To_Expand',sep=';')
      Col_A Column_To_Expand
    0     A                1
    1     A                2
    2     B                3
    3     B                4
    4     B                5
    5     C                6
    '''
    col_order = dataframe.columns.tolist()
    list_col = column_to_expand+'_LIST'
    # Create a separate list column
    dataframe[list_col] = dataframe[column_to_expand].apply(lambda x: [i.strip()
                                                                       for i in str(x).split(sep)])
    s = dataframe[list_col].apply(lambda x: pd.Series(x)).stack()\
        .reset_index(level=1, drop=True)
    new_col_name = 'SINGLE_' + column_to_expand
    s.name = new_col_name
    if drop_index:
        return dataframe.drop([list_col, column_to_expand], axis=1).join(s)\
            .reset_index(drop=True).rename({new_col_name: column_to_expand},
                                           axis='columns')[col_order]
    else:
        return dataframe.drop([list_col, column_to_expand], axis=1).join(s)\
            .rename({new_col_name: column_to_expand},
                    axis='columns')[col_order]


def grouper(raw, split=None, fmt="'{}'", n=10, expand=False):
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


def row_number(dataframe, partition_by, order_by, sort_by=None):
    '''Mimic TSQL Window Functions ROW_NUMBER() OVER(PARTION BY... ORDER BY...).
    Returns a pandas series of integers of windowed numbering 1 to n, n being
    the numbers of members per group.
    dataframe: a pandas DataFrame
    partition_by: a list of columns or string for the partioning of
                  the window grouping
    order_by: a list of columns or string for the ordering of the row numbers
    sort_by: Default None, list of booleans or boolean. Default orders each
             column in order_by ascending. If not using default, sort_by must be
             same length as order_by. True orders descending, False orders
             descending. Each element's index in sort_by corresponds to the same
             index in order_by for sorting.
    examples:
    >>> data = {
    'A': list('aabbe'),
    'B': [1,2,3,4,5],
    'C': [5,4,3,2,1]
    }
    >>> df = pd.DataFrame(data)
    >>> df
        A	B	C
    0	a	1	5
    1	a	2	4
    2	b	3	3
    3	b	4	2
    4	e	5	1
    >>> df['Rank1'] = row_number(df,partition_by='A',order_by=['B','C'])
    >>> df
        A	B	C	Rank1
    0	a	1	5	1
    1	a	2	4	2
    2	b	3	3	1
    3	b	4	2	2
    4	e	5	1	1
    >>> df['Rank2'] = row_number(df,partition_by='A',order_by=['B','C'],
                                 sort_by=[False,False])
    >>> df
        A	B	C	Rank1	Rank2
    0	a	1	5	1	2
    1	a	2	4	2	1
    2	b	3	3	1	2
    3	b	4	2	2	1
    4	e	5	1	1	1
    '''
    if not sort_by:
        return dataframe.sort_values(by=order_by).groupby(partition_by)\
            .cumcount()+1
    else:
        if len(order_by) != len(sort_by):
            raise ValueError("order_by and sort_by must have have the same"
                             " length.")
        if not all(type(i) == bool for i in sort_by):
            raise ValueError('All values in sort_by must be boolean.')
        return dataframe.sort_values(by=order_by, ascending=sort_by)\
            .groupby(partition_by).cumcount()+1
