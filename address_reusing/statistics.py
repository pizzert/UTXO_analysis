from multiprocessing import Pool
import address_reusing.conf as CFG
import pickle as pkl


def compute_stat():

	file = open(CFG.data_path + "tmp_reusing.pkl", 'rb')
	dictionary = pkl.load(file)

	tot_out = 0
	reused_out = 0
	tot_amount = 0
	reused_amount = 0
	tot_address = 0
	reused_address = 0


	for key in dictionary:

		values = dictionary[key]

		tot_address += 1
		tot_amount += values["amount"]
		tot_out += values["n_output"]
		reused = values["reused"]

		if reused:

			print("reused trovato")
			reused_out += values["n_output"]
			reused_amount += values["amount"]
			reused_address += 1

	return {"tot_out": tot_out, "reused_out": reused_out, "tot_amount": tot_amount, "reused_amount": reused_amount, "tot_address": tot_address, "reused_address": reused_address}



def reduce_dictio(dictionary, serialize = False):

	tmp_dictionary = dict()

	print("parte lo scrutinio")
	for key in dictionary:

		reused = dictionary[key]["reused"]

		if reused:

			value = dictionary[key]
			out = value["n_output"]
			amount = value["amount"]
			block = value["block"]


			tmp_dictionary.update({key: {"n_output": out, "amount": amount, "block": block}})

	print("serializzo")
	if serialize:

		with open(CFG.data_path + "analysis/tmp_p2pkh_fee.pkl", 'wb') as ser_file:

			pkl.dump(tmp_dictionary, ser_file)

	return tmp_dictionary


def compute_p2pkh_costo(dictionary, serialize = True):

	fee_dictionary = dict()

	for fee_per_byte in range(CFG.MIN_FEE_PER_BYTE, CFG.MAX_FEE_PER_BYTE, 10):

		print("considero ", fee_per_byte)

		fee_dictionary.update({fee_per_byte: {"amount": 0}})

		for key in dictionary:

			#print("considero", key)

			value = dictionary[key]
			
			amount = value["amount"]
			block = value["block"]
			outputs = value["n_output"]

			if block < 173480:
				lenght = 41 + (138 * outputs) + 33
			else:
				lenght = 41 + (106 * outputs) + 33
			
			dust = amount/lenght

			if dust > fee_per_byte: 
			
				costo = lenght * fee_per_byte
			
				fee_dictionary[fee_per_byte]["amount"] += costo

	if serialize:

		with open(CFG.data_path + "analysis/p2pkh_costo_2.pkl", 'wb') as ser_file:

			pkl.dump(fee_dictionary, ser_file, protocol=2)

	return fee_dictionary


def compute_p2pkh_fee(dictionary, serialize = True):

	fee_dictionary = dict()

	for fee_per_byte in range(CFG.MIN_FEE_PER_BYTE, CFG.MAX_FEE_PER_BYTE, 10):

		print("considero ", fee_per_byte)

		fee_dictionary.update({fee_per_byte: {"amount": 0, "n_output": 0, "addresses": 0}})

		for key in dictionary:

			#print("considero", key)

			value = dictionary[key]
			
			amount = value["amount"]
			block = value["block"]
			outputs = value["n_output"]

			if block < 173480:
				lenght = 41 + (138 * outputs) + 33
			else:
				lenght = 41 + (106 * outputs) + 33

				dust = amount/lenght

				if dust > fee_per_byte:

					print("Non e' dust")

					fee_dictionary[fee_per_byte]["amount"] += amount
					fee_dictionary[fee_per_byte]["n_output"] += outputs
					fee_dictionary[fee_per_byte]["addresses"] += 1

	if serialize:

		with open(CFG.data_path + "analysis/p2pkh_fee.pkl", 'wb') as ser_file:

			pkl.dump(fee_dictionary, ser_file, protocol=2)

	return fee_dictionary


def print_stat(data):

	print("Total number of outputs: ", data["tot_out"])
	print("Total number of addresses: ", data["tot_address"])
	print("Total number of Bitcoin: ", format(float(data["tot_amount"]) / 100000000, '.9f'))
	print("Reused addresses : ", data["reused_address"])
	print("Amount of Bitcoin in reused addresses: ", format(float(data["reused_amount"])/ 100000000, '.9f'))
	print("Number of UTXO associated to reused addresses ", data["reused_out"])




	print("% of reused addresses: ", (float(data["reused_address"]  / data["tot_address"] * 100)))
	print("% of reused output: ", (float(data["reused_out"]  / data["tot_out"] * 100)))
	print("% of Bitcoin at risk: ", (float(data["reused_amount"]  / data["tot_amount"] * 100)))
	
	
















































"""

def parse_dictionary(dictionary):

	total = 0
	reused = 0
	
	for keys in dictionary.keys():

		total= 1

		if dictionary[k] is not None:
			reused += 1

	return total, reused


def par_parse_dictionary():

	with open(CFG.data_path + "tmp_reusing.pkl", 'rb') as file:

		dictionary = pickle.load(file)

	#p = Pool(CFG.POOL_NUMBER)

	total = 0
	reused = 0
	#for x, y in p.imap_unordered(parse_dictionary, dictionary):
	
	parse_dictionary(dictionary)
		#total += total
		#reused += reused


par_parse_dictionary()
"""



