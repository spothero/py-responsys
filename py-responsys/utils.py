def convert_to_list_of_dicts(header_row, data_rows):
    return [dict(zip(header_row, data_row)) for data_row in data_rows]


def convert_to_table_structure(list_of_dicts):
    header_row = sorted(list_of_dicts[0].keys())
    data_rows = []

    for dictionary in list_of_dicts:
        data_row = []

        for key in header_row:
            data_row.append(dictionary.get(key))

        data_rows.append(data_row)

    return header_row, data_rows
