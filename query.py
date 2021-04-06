from constants import TABLE_NAME, DB_NAME, ONE_GB_IN_BYTES

class Query:
    def __init__(self, client):
        self.client = client
        self.paginator = client.get_paginator('query')

    # See records ingested into this table so far
    SELECT_ALL = f"SELECT * FROM {DB_NAME}.{TABLE_NAME}"

    queries = []
    def run_all_queries(self):
        for query_id in range(len(self.queries)):
            print("Running query [%d] : [%s]" % (query_id + 1, self.queries[query_id]))
            self.run_query(self.queries[query_id])
            
    def run_query(self, query_string):
        try:
            page_iterator = self.paginator.paginate(QueryString=query_string)
            for page in page_iterator:
                self.__parse_query_result(page)
        except Exception as err:
            print("Exception while running query:", err)
            #traceback.print_exc(file=sys.stderr)
    
    def __parse_query_result(self, query_result):
        query_status = query_result["QueryStatus"]

        progress_percentage = query_status["ProgressPercentage"]
        print("Query progress so far: " + str(progress_percentage) + "%")
        
        bytes_scanned = query_status["CumulativeBytesScanned"] / ONE_GB_IN_BYTES
        print("Bytes Scanned so far: " + str(bytes_scanned) + " GB")
                
        bytes_metered = query_status["CumulativeBytesMetered"] / ONE_GB_IN_BYTES
        print("Bytes Metered so far: " + str(bytes_metered) + " GB")
        
        column_info = query_result['ColumnInfo']

        print("Metadata: %s" % column_info)
        print("Data: ")
        for row in query_result['Rows']:
            print(self.__parse_row(column_info, row))

    def __parse_row(self, column_info, row):
        data = row['Data']
        row_output = []
        for j in range(len(data)):
            info = column_info[j]
            datum = data[j]
            row_output.append(self.__parse_datum(info, datum))

        return "{%s}" % str(row_output)

    def __parse_datum(self, info, datum):
        if datum.get('NullValue', False):
            return ("%s=NULL" % info['Name'])

        column_type = info['Type']

        # If the column is of TimeSeries Type
        if 'TimeSeriesMeasureValueColumnInfo' in column_type:
            return self.__parse_time_series(info, datum)

        # If the column is of Array Type
        elif 'ArrayColumnInfo' in column_type:
            array_values = datum['ArrayValue']
            return ("%s=%s" % (info['Name'], self.__parse_array(info['Type']['ArrayColumnInfo'], array_values)))

        # If the column is of Row Type
        elif 'RowColumnInfo' in column_type:
            row_coulmn_info = info['Type']['RowColumnInfo']
            row_values = datum['RowValue']
            return self.__parse_row(row_coulmn_info, row_values)

        #If the column is of Scalar Type
        else:
            return self.__parse_column_name(info) + datum['ScalarValue']


    def __parse_time_series(self, info, datum):
        time_series_output = []
        for data_point in datum['TimeSeriesValue']:
            time_series_output.append("{time=%s, value=%s}"
                                      % (data_point['Time'],
                                         self.__parse_datum(info['Type']['TimeSeriesMeasureValueColumnInfo'],
                                                            data_point['Value'])))
        return "[%s]" % str(time_series_output)

    def __parse_column_name(self, info):
        if 'Name' in info:
            return info['Name'] + "="
        else:
            return ""

    def __parse_array(self, array_column_info, array_values):
        array_output = []
        for datum in array_values:
            array_output.append(self.__parse_datum(array_column_info, datum))

        return "[%s]" % str(array_output)
    
    def run_query_with_multiple_pages(self, limit):
        query_with_limit = self.SELECT_ALL + " LIMIT " + str(limit)
        print("Starting query with multiple pages : " + query_with_limit)
        self.run_query(query_with_limit)

    def cancel_query(self):
        print("Starting query: " + self.SELECT_ALL)
        result = self.client.query(QueryString=self.SELECT_ALL)
        print("Cancelling query: " + self.SELECT_ALL)
        try:
            self.client.cancel_query(QueryId=result['QueryId'])
            print("Query has been successfully cancelled")
        except Exception as err:
            print("Cancelling query failed:", err)