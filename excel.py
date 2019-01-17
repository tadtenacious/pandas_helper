import pandas as pd


def write_excel(dataframe, outFile):
    '''Takes in a dataframe and writes it to another file with the correct
    formatting.'''
    if not outFile.endswith('.xlsx'):
        outFile += '.xlsx'
    writer = pd.ExcelWriter(outFile, date_format='m/d/yyyy',
                            datetime_format='m/d/yyyy')
    # writer = pd.ExcelWriter(outFile,date_format='m/d/yyyy',
    #                         datetime_format='m/d/yyyy')
    dataframe.to_excel(writer, index=False)
    wb = writer.book
    ws = writer.sheets['Sheet1']
    format1 = wb.add_format()
    format1.set_num_format('@')  # @ - This is text format in excel
    mny_fmt = wb.add_format({'num_format': '#,##0.00'})
    for i in range(0, len(dataframe.columns)):
        width = max(dataframe.iloc[:, i].fillna('').astype(str).apply(len).max(),
                    len(dataframe.iloc[:, i].name))+8
        width = min(width, 90)
        if dataframe.iloc[:, i].dtype == 'O':
            ws.set_column(i, i, width=width, cell_format=format1)
        elif dataframe.iloc[:, i].name.lower() == 'refund':
            ws.set_column(i, i, width=width, cell_format=mny_fmt)
        else:
            ws.set_column(i, i, width=width)
    ws.autofilter(first_row=0, last_row=0, first_col=0,
                  last_col=len(dataframe.columns)-1)
    ws.freeze_panes(1, 0)
    writer.save()
    return None


def write_sheet(dataframe, writer_obj, sheet_name):
    '''Takes in a dataframe and a writer object and writes an Excel spread sheet with the correct
    formatting.'''
    # writer = pd.ExcelWriter(outFile,date_format='m/d/yyyy',
    #                        datetime_format='m/d/yyyy')
    # writer = pd.ExcelWriter(outFile,date_format='m/d/yyyy',
    #                         datetime_format='m/d/yyyy')
    dataframe.to_excel(writer_obj, index=False, sheet_name=sheet_name)
    wb = writer_obj.book
    ws = writer_obj.sheets[sheet_name]
    format1 = wb.add_format()
    format1.set_num_format('@')  # @ - This is text format in excel
    mny_fmt = wb.add_format({'num_format': '#,##0.00'})
    for i in range(0, len(dataframe.columns)):
        width = max(dataframe.iloc[:, i].fillna('').astype(str).apply(len).max(),
                    len(dataframe.iloc[:, i].name))+8
        width = min(width, 90)
        if dataframe.iloc[:, i].dtype == 'O':
            ws.set_column(i, i, width=width, cell_format=format1)
        elif dataframe.iloc[:, i].name.lower() == 'refund':
            ws.set_column(i, i, width=width, cell_format=mny_fmt)
        else:
            ws.set_column(i, i, width=width)
    ws.autofilter(first_row=0, last_row=0, first_col=0,
                  last_col=len(dataframe.columns)-1)
    ws.freeze_panes(1, 0)
    # writer.save()
    return None
