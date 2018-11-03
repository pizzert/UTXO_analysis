#from bitcoin_tools.analysis.plots import plot_pie
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import my_utxo_anal.conf as CFG
import numpy as np
from bitcoin_tools.analysis.plots import plot_distribution
from bitcoin_tools.analysis.status.plots import plot_dict_from_file
import multiprocessing as mp
import os
import ujson
from math import trunc, floor
import pickle

#####################################################

#generare dizionari dai dati, unire dizionari dopo estrazione


def generate_dictionary(file, key = "tx_height", tx_type = "P2PK"):

	fin = open(CFG.parallel_data_path + tx_type + "/" + file, 'r')

	values = dict()
	for line in fin:

		data = ujson.loads(line[:-1])
		block = data[key]

		if block in values:

			values[block] += 1
		
		else:

			values[block] = 1

	return values

def compare_dictionary(main, tmp):

	for key in tmp:
		
		if key in main:

			main[key] += tmp[key]
		
		else:
			
			main.update({key: tmp[key]})

	return main

def merge_dictionaries(dict_list):

	main = dict_list[0]
	del dict_list[0]

	for dict_name in dict_list:

		main = compare_dictionary(main, dict_name)

	return main

def aggregate_keys(dictionary, step = 1000):

	res_dict = dict()
	for key in dictionary:

		value = dictionary[key]
		tmp_key = trunc(floor(key/step)*step)

		if tmp_key in res_dict:

			res_dict[tmp_key] += value
		else:
			res_dict[tmp_key] = value

	return res_dict

def plot_dictionary(dictionary, title, xlabel, ylabel, aggregation = False, agg_step = 1000):

	if aggregation:

		dictionary = aggregate_keys(dictionary, step = agg_step)


	x_value = dictionary.keys()
	x_value.sort()

	y_value = []
	for keys in x_value:

		y_value += [dictionary[keys]]

	plot_distribution(x_value, y_value, title, xlabel, ylabel, log_axis = '') 



def plot_pie(values, labels, title,  colors = ['r', 'orange', 'g', 'y', 'b'], explode=None, autopct=None, pctdistance=1.3, startangle=None, wedgeprops=None, frame=False, font_size=20):

	fig = plt.figure()
	ax = plt.subplot(111)


	ax.pie(values, colors=colors, startangle=90, pctdistance=1.3, wedgeprops={'linewidth': 0}, explode = [0.2, 0.1, 0.1, 0.1, 0])
	s = float(np.sum(values))
	perc = [v/s*100 for v in values]
	plt.legend(loc="best", labels=['%s, %1.1f %%' % (l, s) for l, s in zip(labels, perc)], fontsize="x-small")

	ax.axis('equal')

	plt.title(title, {'color': 'k', 'fontsize': font_size})
	fig.savefig(CFG.parallel_data_path + "figures/" + title + ".png")
	plt.show()

def plot_amount(amount):

	labels = amount.keys()
	labels.remove("Total")


	coin_value = [amount.get(k).get("amount") for k in labels]
	output_number = [amount.get(k).get("total_output") for k in labels]
	dust_number = [amount.get(k).get("dust_output") for k in labels]
	dust_value = [amount.get(k).get("dust_amount") for k in labels]

	plot_pie(coin_value, labels, "bitcoin repartition")
	plot_pie(output_number, labels, "output repartition")
	plot_pie(dust_number, labels, "repartition of dust output")
	plot_pie(dust_value, labels, "dust repartition")



def plot_dust(dictionary):

	amount = dict()
	n_out = dict()

	for key in dictionary:

		tmp_amount = dictionary[key]["amount"]
		amount.update({key: tmp_amount})
		tmp_out = dictionary[key]["n_output"]
		n_out.update({key: tmp_out})

	plot_dictionary(n_out, "Dust evolution per feerate", "fee (satoshi)", "number of output")
	plot_dictionary(amount, "Dust evolution per feerate", "fee (satoshi)", "amount")
	
		








	





with open(CFG.parallel_data_path +"analysis/" + "tmp_P2PK_block_amount.pkl", 'rb') as ser_file:
	
	data = pickle.load(ser_file)

plot_dictionary(data, "P2PK amount per Block Height", "block number", "amount (satoshi)", aggregation = False)


with open(CFG.parallel_data_path +"analysis/" + "tmp_P2MS_block_amount.pkl", 'rb') as ser_file:
	
	data = pickle.load(ser_file)

plot_dictionary(data, "P2MS amount per Block Height", "block number", "amount (satoshi)", aggregation = False)


with open(CFG.parallel_data_path +"analysis/" + "tmp_P2SH_block_amount.pkl", 'rb') as ser_file:
	
	data = pickle.load(ser_file)

plot_dictionary(data, "P2SH amount per Block Height", "block number", "amount (satoshi)", aggregation = False)


with open(CFG.parallel_data_path +"analysis/" + "tmp_NSTD_block_amount.pkl", 'rb') as ser_file:
	
	data = pickle.load(ser_file)

plot_dictionary(data, "NSTD amount per Block Height", "block number", "amount (satoshi)", aggregation = False)


with open(CFG.parallel_data_path +"analysis/" + "tmp_P2PKH_block_amount.pkl", 'rb') as ser_file:
	
	data = pickle.load(ser_file)

plot_dictionary(data, "P2PKH amount per Block Height", "block number", "amount (satoshi)", aggregation = False)
















"""
with open(CFG.parallel_data_path +"analysis/" + "tmp_P2PK.pkl", 'rb') as ser_file:
	
	data = pickle.load(ser_file)

data = round_dust(data)
plot_dust(data, "P2PK: output per fee", "P2PK_dust")

"""

"""
with open(CFG.parallel_data_path + "analysis/" + "tmp_P2PKH.pkl", 'rb') as ser_file:
	
	data = pickle.load(ser_file)

data = round_dust(data)
plot_dust(data, "P2PKH: output per fee", "P2PKH_dust")

with open(CFG.parallel_data_path + "analysis/" + "tmp_P2MS.pkl", 'rb') as ser_file:
	
	data = pickle.load(ser_file)

data = round_dust(data)
plot_dust(data, "P2MS: output per fee", "P2MS_dust")


with open(CFG.parallel_data_path + "analysis/" + "tmp_P2SH.pkl", 'rb') as ser_file:
	
	data = pickle.load(ser_file)

data = round_dust(data)
plot_dust(data, "P2SH: output per fee", "P2SH_dust")

with open(CFG.parallel_data_path + "analysis/" + "tmp_NSTD.pkl", 'rb') as ser_file:
	
	data = pickle.load(ser_file)

data = round_dust(data)
plot_dust(data, "NSTD: output per fee", "NSTD_dust")


"""















#plot_ex("numero di riuso fra gli indirizzi piu' ricchi","value", "# reused tx/ addr(?)")




#with open(CFG.parallel_data_path +"analysis/" + "tmp_P2PK_block.pkl", 'rb') as ser_file:
#	
#	data = pickle.load(ser_file)
#
#plot_block(data, "P2PK UTXO per block height")

#with open(CFG.parallel_data_path +"analysis/" + "tmp_P2PKH_block.pkl", 'rb') as ser_file:
#	
#	data = pickle.load(ser_file)
#
#plot_block(data, "P2PKH UTXO per block height")
#
#with open(CFG.parallel_data_path +"analysis/" + "tmp_P2MS_block.pkl", 'rb') as ser_file:
#	
#	data = pickle.load(ser_file)
#
#plot_block(data, "P2MS UTXO per block height")
#
#with open(CFG.parallel_data_path +"analysis/" + "tmp_P2SH_block.pkl", 'rb') as ser_file:
#	
#	data = pickle.load(ser_file)
#
#plot_block(data, "P2SH UTXO per block height")
#
#with open(CFG.parallel_data_path +"analysis/" + "tmp_NSTD_block.pkl", 'rb') as ser_file:
#	
#	data = pickle.load(ser_file)
#
#plot_block(data, "NSTD UTXO per block height")











#bar_from_dict(data, "dust per output")











"""
input_type = [94327, 40931793, 9914025, 409339, 472840]

label_type = ['P2PK', 'P2PKH', 'P2SH','P2MS', 'NSTD' ]

colors = ['r', 'orange', 'g', 'y', 'b']

explode = [0.1, 0, 0, 0, 0]

plot_pie(input_type, label_type, colors, "UTXO repartition") 
"""
