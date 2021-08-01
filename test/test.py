import csv
import json
from string import ascii_letters as eng_letters
from random import choice, randint, shuffle
from xml.etree.ElementTree import Element, tostring
from xml.dom import minidom


class GenerateTestData():
    '''Получает на вход параметры генерации данных:
    row_count,
    row_duplicate_count,
    columns_count,
    extra_M_count,
    extra_D_count,
    '''
    def __init__(self):
        self.filename_list = [
            'csv_data_1.csv',
            'csv_data_2.csv',
            'json_data.json',
            'xml_data.xml',
        ]
        self.rows_count = 10
        self.extra_rows_count = 5
        self.columns_count = 3
        self.D_range = tuple(set(eng_letters.lower()))
        self.M_range = [i for i in range(5)]
        self.advanced_data_dict = {}
        self.basic_data_items = []
        self.list_of_dict = []
        self.chunked_data = []
        self.columns_names = []
        self.D_columns = []
        self.M_columns = []

    def create_test_files(self):
        self.set_columns_names()
        self.generate_advanced_data()
        advanced_text = self.dict_to_text()
        self.create_tsv('advanced_expected.tsv', advanced_text)
        self.add_extra_rows()
        basic_text = self.list_to_text()
        self.create_tsv('basic_expected.tsv', basic_text)
        self.shuffle_and_chunk()
        self.write_to_files()

    def set_columns_names(self):
        self.D_columns = tuple('D'+str(i+1) for i in range(self.columns_count))
        self.M_columns = tuple('M'+str(i+1) for i in range(self.columns_count))
        self.columns_names = self.D_columns + self.M_columns

    def generate_advanced_data(self):
        '''Возвращает словарь

        {(s, ..., s): (i, ..., i),
        ...
        }
        '''
        for _ in range(self.rows_count):
            keys = tuple(choice(self.D_range) for _ in range(self.columns_count))
            values = tuple(choice(self.M_range) for _ in range(self.columns_count))
            self.advanced_data_dict[keys] = values

    def dict_to_text(self):
        advanced_data_list = []
        columns_row = '\t'.join(self.columns_names)
        advanced_data_list.append(columns_row)
        advanced_data_dict_items = sorted(list(self.advanced_data_dict.items()), key=lambda row: row[0][0])
        for key, value in advanced_data_dict_items:
            row = '\t'.join(key) + '\t' + '\t'.join(map(str, value))
            advanced_data_list.append(row)
        return '\n'.join(advanced_data_list)

    @staticmethod
    def create_tsv(filename, text):
        with open(filename, 'w') as file:
            file.write(text)

    def add_extra_rows(self):
        self.basic_data_items = list(self.advanced_data_dict.items())
        for i in range(self.extra_rows_count):
            element_num = randint(0, len(self.basic_data_items)-1)
            keys, values = self.basic_data_items[element_num]
            values_dupl_1 = tuple(randint(0, value) for value in values)
            values_dupl_2 = tuple(values[i] - values_dupl_1[i]
                                  for i, _ in enumerate(values))
            del self.basic_data_items[element_num]
            self.basic_data_items.append((keys, values_dupl_1))
            self.basic_data_items.append((keys, values_dupl_2))

    def list_to_text(self):
        basic_data_list = []
        columns_row = '\t'.join(self.columns_names)
        basic_data_list.append(columns_row)
        basic_data_items = sorted(self.basic_data_items, key=lambda row: row[0][0])
        for key, value in basic_data_items:
            row = '\t'.join(key) + '\t' + '\t'.join(map(str, value))
            basic_data_list.append(row)
        return '\n'.join(basic_data_list)

    def shuffle_and_chunk(self):
        shuffle(self.basic_data_items)
        chunk_count = len(self.filename_list)
        self.chunked_data = [self.basic_data_items[i::chunk_count] for i in range(chunk_count)]

    def choose_writer(self, filename):
        '''Возвращает функцию для записи в файл соответствующего расширения

        filename: str
            путь до считываемого файла
        '''
        if filename[-4:] == '.csv':
            return self.write_to_csv
        elif filename[-4:] == '.xml':
            return self.write_to_xml
        elif filename[-5:] == '.json':
            return self.write_to_json
        elif filename[-5:] == '.yaml':
            print('Расширение .yaml ещё не поддерживается')
            return None
        else:
            print('Расширение файла', filename, 'ещё поддерживается')
            return None

    def to_list_of_dict(self, data):
        if type(data) is dict:
            data = data.items()
        list_of_dict = []
        for d_values, m_values in data:
            row_dict = {}
            for i, column in enumerate(self.D_columns):
                row_dict[column] = d_values[i]
            for i, column in enumerate(self.M_columns):
                row_dict[column] = m_values[i]
            list_of_dict.append(row_dict)
        return list_of_dict

    def write_to_files(self):
        for i, filename in enumerate(self.filename_list):
            writer = self.choose_writer(filename)
            if writer:
                writer(filename, self.chunked_data[i])

    def write_to_csv(self, filename, list_of_dict):
        list_of_dict = self.to_list_of_dict(list_of_dict)
        with open(filename, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=self.columns_names)
            writer.writeheader()
            writer.writerows(list_of_dict)

    def write_to_json(self, filename, list_of_dict):
        list_of_dict = self.to_list_of_dict(list_of_dict)
        json_dict = {'fields': list_of_dict}
        json_text = json.dumps(json_dict, indent=4)
        with open(filename, 'w') as file:
            file.write(json_text)

    def write_to_xml(self, filename, list_of_dict):
        list_of_dict = self.to_list_of_dict(list_of_dict)

        def dict_to_xml(list_of_dict):
            root_elem = Element('root')
            for row_dict in list_of_dict:
                objects_elem = Element('objects')
                for key, val in row_dict.items():
                    elem = Element('object')
                    elem.set('name', key)
                    value_elem = Element('value')
                    value_elem.text = str(val)
                    elem.append(value_elem)
                    objects_elem.append(elem)
                root_elem.append(objects_elem)
            root_elem.append(objects_elem)
            return root_elem

        data_xml = dict_to_xml(list_of_dict)
        dom = minidom.parseString(tostring(data_xml, encoding='unicode'))
        with open(filename, 'w', newline='') as file:
            file.write(dom.toprettyxml(indent='    '))


if __name__ == '__main__':
    generator = GenerateTestData()
    generator.create_test_files()
