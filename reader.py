import io
import pandas as pd
import PyPDF2
import tabula

import string
from os import listdir

class Reader(object):
	def __init__(self, file_path, date):
		"""
		Read the pdf file to get the number of pages
		This can be deleted if the state continues with
		the one page tamplate
		"""

		# Process the date
		self.lst_date = date.split('.')
		self.date = f'{self.lst_date[2]}-{self.lst_date[1]}-{self.lst_date[0]}'

		# Create the tabula dataframe (which is also a pandas dataframe)
		self.reader_df = tabula.read_pdf(file_path, pages="all")
		self.df = self.reader_df[-1]
		# RENAME the columns
		self.df.columns = ["Cities-0", "Deaths-0",
			"Cities-1", "Cities-1-cases", "Deaths-1",
			"Cities-2", "Cities-2-cases", "Deaths-2"]

	def getDict(self):
		"""
		Now that I have the dataset, create the dictionary

		Using a list of dicts
		"""
		# Create a list to hold the dictionaries
		list_of_dicts = []
		
		# The data (which is the same for all)
		# Is defined in the class
		
		# The State (which here is MG for all)
		state = "MG"
		
		# The cities
		# Iterate first through cities-0 and deaths-0
		# The number of cases is in cities-0
		cities_0 = self.df["Cities-0"].tolist()
		deaths_0 = self.df["Deaths-0"].tolist()

		# Need to load the deaths_2 as well, as there appears to be
		# an error while reading
		deaths_2 = self.df["Deaths-2"].tolist()

		# Iterate through the first col of cities
		size = len(cities_0)
		for i in range(1, size):
			# Check if there is some city there
			# Apparently, nan is of type float...
			if isinstance(cities_0[i], float):
				pass
			else:
				# Create the dictionary for the city
				c_dict = {"date": self.date, "state": "MG",
					"place_type": "city"}
				
				# Split in the space
				c_lst = cities_0[i].split(" ")
				if c_lst[-1] == '-':
					c_cases = 0
				else:
					c_cases = int(c_lst[-1])

				# rstrip to remove the space at the end
				c_name = ''.join(i + " " for i in c_lst[:-1]).rstrip()

				# Check if the value for deaths_2 was written here
				if not c_name[0].isalpha():
					deaths_2_value = ''.join(i for i in c_name
						if not(i.isalpha()))
					deaths_2[i] = deaths_2_value
					nc_name = ''.join(i for i in c_name
						if i.isalpha() or i == ' ')
					c_name = nc_name
				
				# Add them to the c_dict
				c_dict["city"] = c_name
				c_dict["confirmed"] = int(c_cases)

				# Get the deaths
				if deaths_0[i] == '-':
					c_dict["deaths"] = 0
				else:
					c_dict["deaths"] = int(deaths_0[i])

				# Add the c_dict to the list_of_dicts
				list_of_dicts.append(c_dict)


		# The middle part is well organized
		cities_1 = self.df["Cities-1"].tolist()
		cities_1_cases = self.df["Cities-1-cases"].tolist()
		deaths_1 = self.df["Deaths-1"].tolist()

		# Iterate through the middle col (same size)
		for i in range(1, size):
			# Check if there is some city there
			# Apparently, nan is of type float...
			if isinstance(cities_1[i], float):
				pass
			else:
				# Create the dictionary for the city
				c_dict = {"date": self.date, "state": "MG",
					"place_type": "city"}
				
				# Add them to the c_dict
				c_dict["city"] = cities_1[i]
				if cities_1_cases[i] == '-':
					c_dict["confirmed"] = 0
				else:
					c_dict["confirmed"] = int(cities_1_cases[i])

				# Get the deaths. If could be nan
				if deaths_1[i] == '-' or isinstance(deaths_1[i], float):
					c_dict["deaths"] = 0
				else:
					c_dict["deaths"] = int(deaths_1[i])

				# Add the c_dict to the list_of_dicts
				list_of_dicts.append(c_dict)


		# The last part is corrected while processing part 0
		cities_2 = self.df["Cities-2"].tolist()
		cities_2_cases = self.df["Cities-2-cases"].tolist()
		deaths_2 = self.df["Deaths-2"].tolist()

		# Iterate through them (same size)
		for i in range(1, size):
			# Check if there is some city there
			# if cities_0[i] == 'nan':
			# Apparently, nan is of type float...
			if isinstance(cities_2[i], float):
				pass
			else:
				# Create the dictionary for the city
				c_dict = {"date": self.date, "state": "MG",
					"place_type": "city"}
				
				# Add them to the c_dict
				c_dict["city"] = cities_2[i]
				if cities_2_cases[i] == '-':
					c_dict["confirmed"] = 0
				else:
					c_dict["confirmed"] = int(cities_2_cases[i])

				# Get the deaths. If could be nan
				if deaths_2[i] == '-' or isinstance(deaths_2[i], float):
					c_dict["deaths"] = 0
				else:
					c_dict["deaths"] = int(deaths_2[i])

				# Add the c_dict to the list_of_dicts
				list_of_dicts.append(c_dict)

		# The confirmed cases for the entire state
		N_total_cases = self.df["Cities-1-cases"].tolist()[0]
		total_cases = ''.join(c for c in N_total_cases.split("=")[-1]
			if c not in string.punctuation)

		# The number of deaths for the cities

		# The number of deaths for the entire state
		N_total_deaths = self.df["Deaths-0"].tolist()[0]
		total_deaths = ''.join(c for c in N_total_deaths.split("=")[-1]
			if c not in string.punctuation)

		# Create the state value
		list_of_dicts.append({"date": self.date,
			"state": state, "city": None, "place_type": "state",
			"confirmed": int(total_cases), "deaths": int(total_deaths)})
		
		for d in list_of_dicts:
			yield {
				"city": d['city'],
                "confirmed": d['confirmed'],
                "date": d['date'],
                "deaths": d['deaths'],
                "place_type": d['place_type'],
                "state": d['state'],
			}


if __name__ == "__main__":
	r = Reader("pdfs/24.04.2020_Boletim_epidemiologico_COVID-19_MG.pdf",
		"24.04.2020")
	y = r.getDict()
	for i in y:
		print(i)