import json
import csv
import xml.etree.ElementTree as ET


class ToOneFile():
	def __init__(self, columns_reference='xml_data.xml'):
		self.filelist = [
			'csv_data_1.csv',
			'csv_data_2.csv',
			'json_data.json',
			'xml_data.xml',
		]
		self.basic_filename = 'basic_results.tsv'
		self.advanced_filename = 'advanced_results.tsv'

		self.columns_ordered = ()
		self.columns_key = ()
		self.columns_value = ()

		self.columns_reference = columns_reference
		self.data_list = []
		self.data_ordered = []
		self.without_duplicate = {}

	def write_files(self):
		'''Последовательная активация функций для
		формирования файлов basic_results.tsv, advanced_results.tsv
		'''
		self.get_columns(self.columns_reference)
		self.read_files(self.filelist)
		self.make_data_ordered()
		self.make_dict_without_duplicate()

		data_text_basic = self.list_ordered_to_text()
		self.write_to_tsv(self.basic_filename, data_text_basic)

		data_text_advanced = self.dict_without_duplicate_to_text()
		self.write_to_tsv(self.advanced_filename, data_text_advanced)

	def get_columns(self, ref):
		'''Формирует кортеж названий столбцов,
		которые будут записаны в итоговый файл

		ref: str
			путь до файла с эталонными названиями столбцов
		'''
		self.columns_ordered = self.read_files([ref], only_columns=True)
		self.columns_key = tuple(col for col in self.columns_ordered if col.startswith('D'))
		self.columns_value = tuple(col for col in self.columns_ordered if col.startswith('M'))

	def read_files(self, filelist, only_columns=False):
		'''Возвращает неупорядоченный список словарей
		считанных со всей файлов из filelist

		filelist: list
			содержит пути до считываемых файлов
		only_columns: bool
			определяет считать названия столбцов или весь файл
		'''
		for filename in filelist:
			reader = self.choose_reader(filename)
			if only_columns:
				return reader(filename, only_columns)
			if reader:
				self.data_list += reader(filename, only_columns)
		return self.data_list

	def make_data_ordered(self):
		'''Возвращает список кортежей, с упорядоченными строками и столбцами

		Имеет структуру [((s, s, ..., s), (i, i, ..., i)),
							...
						]
		'''
		for row in self.data_list:
			row_key = tuple(row[col] for col in self.columns_key)
			row_value = tuple(row[col] for col in self.columns_value)
			self.data_ordered.append((row_key, row_value))
		self.data_ordered = sorted(self.data_ordered, key=lambda row: row[0][0])
		return self.data_ordered

	def make_dict_without_duplicate(self):
		'''Возвращает список словарей, с удаленными дубликатами
		Числовые значения дубликатов суммируются

		Имеет структуру [{(s, s, ..., s): (i, i, ..., i)},
							...
						]
		'''
		for row in self.data_ordered:
			keys, values = row[0], row[1]
			if keys in self.without_duplicate:
				value = tuple(sum(x) for x in zip(values, self.without_duplicate[keys]))
				self.without_duplicate[keys] = value
			else:
				self.without_duplicate[keys] = values
		return self.without_duplicate

	def list_ordered_to_text(self):
		'''Преобразует список кортежей в текст для сохранения в формате .tsv'''
		data_row_text = []
		for row in self.data_ordered:
			row_key = '\t'.join(row[0])
			row_value = '\t'.join(map(str, row[1]))
			row_text = row_key + '\t' + row_value
			data_row_text.append(row_text)
		data_text = '\n'.join(data_row_text)
		return data_text

	def dict_without_duplicate_to_text(self):
		'''Преобразует список словарей в текст для сохранения в формате .tsv'''
		data_row_text = []
		for key in self.without_duplicate:
			row_key = '\t'.join(key)
			row_value = '\t'.join(map(str, self.without_duplicate[key]))
			row_text = row_key + '\t' + row_value
			data_row_text.append(row_text)
		data_text = '\n'.join(data_row_text)
		return data_text

	def write_to_tsv(self, filename, data_text):
		'''Запись текста data_text в файл filename'''
		result_text = '\t'.join(self.columns_ordered) + '\n' + data_text
		with open(filename, 'w') as file:
			file.write(result_text)

	def choose_reader(self, filename):
		'''Возвращает функцию для чтения из файла соответствующего расширения

		filename: str
			путь до считываемого файла
		'''
		if filename[-4:] == '.csv':
			return self.from_csv
		elif filename[-4:] == '.xml':
			return self.from_xml
		elif filename[-5:] == '.json':
			return self.from_json
		elif filename[-5:] == '.yaml':
			print('Расширение .yaml ещё не поддерживается')
			return None
		else:
			print('Расширение файла', filename, 'не поддерживается')
			return None

	def from_csv(self, filename, only_columns=False):
		'''Возвращает список кортежей, считанных из csv-файла

		filename: str
			путь до считываемого файла
		only_columns: bool
			определяет считать названия столбцов или весь файл
		'''
		with open(filename, newline='') as csvfile:
			data_from_csv_file = csv.reader(csvfile, delimiter=',')
			csv_data = []
			csv_columns = next(data_from_csv_file)
			if only_columns:
				return csv_columns
			for row in data_from_csv_file:
				object_dict = {}
				for i, _ in enumerate(row):
					col_name = csv_columns[i]
					if col_name in self.columns_ordered:
						value = row[i]
						if col_name.startswith('M'):
							value = self.try_to_int(value)
						if value is None:
							print('В файле', filename, 'в столбце', col_name, 'некорректное значение')
						else:
							object_dict[col_name] = value
				csv_data.append(object_dict)
		return csv_data

	def from_xml(self, filename, only_columns=False):
		'''Возвращает список кортежей, считанных из xml-файла

		filename: str
			путь до считываемого файла
		only_columns: bool
			определяет считать названия столбцов или весь файл
		'''
		tree = ET.parse(filename)
		root = tree.getroot()
		xml_data = []
		for objects in root:
			object_dict = {}
			for object_ in objects:
				name = object_.get('name')
				value = object_.find('value').text
				if name.startswith('M'):
					value = self.try_to_int(value)
				if value is None:
					print('В файле', filename, 'в столбце', name, 'некорректное значение')
				else:
					object_dict[name] = value
			if only_columns:
				return list(object_dict.keys())
			xml_data.append(object_dict)
		return xml_data

	def from_json(self, filename, only_columns=False):
		'''Возвращает список кортежей, считанных из json-файла

		filename: str
			путь до считываемого файла
		only_columns: bool
			определяет считать названия столбцов или весь файл
		'''
		with open(filename) as file:
			json_data = json.load(file)['fields']
			if only_columns:
				return list(json_data[0].keys())
		return json_data

	@staticmethod
	def try_to_int(value):
		try:
			return int(value)
		except ValueError:
			return None


if __name__ == '__main__':
	ex = ToOneFile()
	ex.write_files()
