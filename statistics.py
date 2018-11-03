from my_utxo_anal import CFG
from my_utxo_anal.plot import *
import ujson
from functools import partial
from multiprocessing import Pool, Process
from bitcoin_tools.analysis.status.utils import roundup_rate
from itertools import product
import cPickle as pickle
import os

def merge_fee(dict1, dict2):

	return {k: dict1.get(k, 0) + dict2.get(k, 0) for k in set(dict1) | set(dict2)}

def print_stat(amount):

	#plot_amount(amount)

	print "Total amount of Bitcoin in Chainstate: ", 									format(float(amount["Total"]["amount"]) / 100000000, '.9f')
	print "Total amount of Bitcoin in P2PK output: ", 								format(float(amount["P2PK"]["amount"])  / 100000000, '.9f')
	print "Total amount of Bitcoin in P2PKH outpput: ", 						format(float(amount["P2PKH"]["amount"]) / 100000000, '.9f')
	print "Total amount of Bitcoin in P2SH output: ", 								format(float(amount["P2SH"]["amount"])  / 100000000, '.9f')
	print "Total amount of Bitcoin in P2MS output: ", 								format(float(amount["P2MS"]["amount"])  / 100000000, '.9f')
	print "Total amount of Bitcoin in non standard output: ", format(float(amount["NSTD"]["amount"])  / 100000000, '.9f'), "\n"

	print "Total amount of dust in P2PK output: ", 						  format(float(amount["P2PK"]["dust_amount"] )/ 100000000, '.9f')
	print "Total amount of dust in P2PKH outpput: ", 				  format(float(amount["P2PKH"]["dust_amount"])/ 100000000, '.9f')
	print "Total amount of dust in P2SH output: ", 						  format(float(amount["P2SH"]["dust_amount"] )/ 100000000, '.9f')
	print "Total amount of dust in P2MS output: ", 						  format(float(amount["P2MS"]["dust_amount"] )/ 100000000, '.9f')
	print "Total amount of dust in non standard output: ", format(float(amount["NSTD"]["dust_amount"] )/ 100000000, '.9f'), "\n"
	print "Total amount of dust in chainstate: ", 									format(float(amount["Total"]["dust_amount"] )/ 100000000, '.9f'), "\n"

	print "Total number of output: ", 													amount["Total"]["total_output"]
	print "Total	number of P2PK output: ", 								amount["P2PK"]["total_output"]
	print "Total	number of P2PKH output: ", 							amount["P2PKH"]["total_output"]
	print "Total	number of P2SH output: ", 								amount["P2SH"]["total_output"]
	print "Total	number of P2MS output: ", 								amount["P2MS"]["total_output"]
	print "Total	number of non standard output: ", amount["NSTD"]["total_output"], "\n"

	print "Total	number of dust P2PK output: ", 								amount["P2PK"]["dust_output"]
	print "Total	number of dust P2PKH output: ", 							amount["P2PKH"]["dust_output"]
	print "Total	number of dust P2SH output: ", 								amount["P2SH"]["dust_output"]
	print "Total	number of dust P2MS output: ", 								amount["P2MS"]["dust_output"]
	print "Total	number of dust non standard output: ", amount["NSTD"]["dust_output"]
	print "Total number of dust output: ",      								amount["Total"]["dust_output"], "\n"

	print "=================================================="
	print "---------------- UTXO Repartition ----------------"
	print "================================================== \n"

	print	"% of existing Bitcoin in P2PK output: ", 								(float(amount["P2PK"]["amount"]) / amount["Total"]["amount"] * 100)
	print	"% of existing Bitcoin in P2PKH output: ",								(float(amount["P2PKH"]["amount"]) / amount["Total"]["amount"] * 100)
	print	"% of existing Bitcoin in P2SH output: ", 								(float(amount["P2SH"]["amount"]) / amount["Total"]["amount"] * 100)
	print	"% of existing Bitcoin in P2MS output: ", 								(float(amount["P2MS"]["amount"]) / amount["Total"]["amount"] * 100)
	print	"% of existing Bitcoin in non standard output: ", (float(amount["NSTD"]["amount"]) / amount["Total"]["amount"] * 100), "\n"

	print	"% of P2PK output: ",  							format(float(amount["P2PK"]["total_output"]) / amount["Total"]["total_output"] * 100)
	print	"% of P2PKH output: ", 							format(float(amount["P2PKH"]["total_output"]) / amount["Total"]["total_output"] * 100)
	print	"% of P2SH output: ",  							format(float(amount["P2SH"]["total_output"]) / amount["Total"]["total_output"] * 100)
	print	"% of P2MS output: ",  							format(float(amount["P2MS"]["total_output"]) / amount["Total"]["total_output"] * 100)
	print	"% of non standard output: ", format(float(amount["NSTD"]["total_output"]) / amount["Total"]["total_output"] * 100), "\n"

	print "% of dust in P2PK output: ", 						    format(float(amount["P2PK"]["dust_amount"] ) / amount["P2PK"]["amount"] * 100, '.9f')
	print "% of dust in P2PKH outpput: ", 				    format(float(amount["P2PKH"]["dust_amount"]) / amount["P2PKH"]["amount"] * 100, '.9f')
	print "% of dust in P2SH output: ", 						    format(float(amount["P2SH"]["dust_amount"] ) / amount["P2SH"]["amount"] * 100, '.9f')
	print "% of dust in P2MS output: ", 						    format(float(amount["P2MS"]["dust_amount"] ) / amount["P2MS"]["amount"] * 100, '.9f')
	print "% of dust in non standard output: ",   format(float(amount["NSTD"]["dust_amount"] ) / amount["NSTD"]["amount"] * 100, '.9f')
	print "Total amount of dust in chainstate: ", format(float(amount["Total"]["dust_amount"])/ amount["Total"]["amount"] * 100, '.9f'), "\n"

	print "% of dust P2PK output: ", 						  format(float(amount["P2PK"]["dust_output"] ) / amount["P2PK"]["total_output"]  * 100, '.9f')
	print "% of dust P2PKH outpput: ", 				  format(float(amount["P2PKH"]["dust_output"]) / amount["P2PKH"]["total_output"] * 100, '.9f')
	print "% of dust P2SH output: ", 						  format(float(amount["P2SH"]["dust_output"] ) / amount["P2SH"]["total_output"]  * 100, '.9f')
	print "% of dust P2MS output: ", 						  format(float(amount["P2MS"]["dust_output"] ) / amount["P2MS"]["total_output"]  * 100, '.9f')
	print "% of dust non standard output: ", format(float(amount["NSTD"]["dust_output"] ) / amount["NSTD"]["total_output"]  * 100, '.9f')
	print "% of dust in chainstate: ", 						format(float(amount["Total"]["dust_amount"]) / amount["Total"]["total_output"] * 100, '.9f'), "\n"

def IsDust(dust, feerate):

	if dust > CFG.MAX_FEE_PER_BYTE:
		return False
	elif dust < CFG.MIN_FEE_PER_BYTE:
		return True
	elif dust < feerate:
		return True
	else:
		return False

def merge_block_height(dict1, dict2):

	return {k: dict1.get(k, 0) + dict2.get(k, 0) for k in set(dict1) | set(dict2)}

def stat_per_UTXO(fin_name, output_name, feerate = CFG.AVG_FEE_PER_BYTE):

	fin = open(CFG.parallel_data_path + output_name + "/" + fin_name, 'r')


	n_output = 0
	btc_value = 0
	n_dust_output = 0
	total_btc_dust = 0
	out_per_fee = dict()
	out_per_block = dict()
	amount_per_block = dict()

	for line in fin:

		n_output += 1
		
		data = ujson.loads(line[:-1])
		fee = data["dust"]
		block = data["tx_height"]
		amount = data["amount"]


		if fee in out_per_fee:
			out_per_fee[fee] += 1
		else:
			out_per_fee.update({fee : 1})

		if block in out_per_block:
			out_per_block[block] += 1
		
		else:
			out_per_block.update({block : 1})	


		if block in amount_per_block:

			amount_per_block[block] += amount

		else:
			amount_per_block.update({block: amount})




		if IsDust(fee, feerate):
			n_dust_output += 1
			total_btc_dust += data["amount"]

		btc_value += data["amount"]

		#print out_per_fee

	fin.close()
	
	return btc_value, n_output, n_dust_output, total_btc_dust, out_per_fee, out_per_block, amount_per_block

def compute_stat():

	amount={
										"P2PK": {"amount": 0, "total_output": 0, "dust_output": 0, "dust_amount": 0},
									"P2PKH": {"amount": 0, "total_output": 0, "dust_output": 0, "dust_amount": 0},
										"P2MS": {"amount": 0, "total_output": 0, "dust_output": 0, "dust_amount": 0},
										"P2SH": {"amount": 0, "total_output": 0, "dust_output": 0, "dust_amount": 0},
										"NSTD": {"amount": 0, "total_output": 0, "dust_output": 0, "dust_amount": 0},
									"Total": {"amount": 0, "total_output": 0, "dust_output": 0, "dust_amount": 0}
								}



	parsed_P2PK_files_input  = os.listdir(CFG.parallel_data_path + "/P2PK" )
	parsed_P2PKH_files_input = os.listdir(CFG.parallel_data_path + "/P2PKH")
	parsed_P2MS_files_input  = os.listdir(CFG.parallel_data_path + "/P2MS" )
	parsed_P2SH_files_input  = os.listdir(CFG.parallel_data_path + "/P2SH" )
	parsed_NSTD_files_input  = os.listdir(CFG.parallel_data_path + "/NSTD" )

	P2PK_amount  = partial(stat_per_UTXO, output_name="P2PK" )
	P2PKH_amount = partial(stat_per_UTXO, output_name="P2PKH")
	P2MS_amount  = partial(stat_per_UTXO, output_name="P2MS" )
	P2SH_amount  = partial(stat_per_UTXO, output_name="P2SH" )
	NSTD_amount  = partial(stat_per_UTXO, output_name="NSTD" )

	p = Pool(CFG.POOL_NUMBER)

	
	P2PK_fee = dict()
	P2PK_block = dict()
	P2PK_block_amount = dict()

	print "analizzo P2PK"
	for btc_value, n_output, n_dust_output, total_btc_dust, out_per_fee, out_per_block, amount_per_block in p.imap_unordered(P2PK_amount, parsed_P2PK_files_input):
		
		P2PK_fee = merge_fee(P2PK_fee, out_per_fee)
		P2PK_block = merge_block_height(P2PK_block, out_per_block)
		P2PK_block_amount = merge_block_height(P2PK_block_amount, amount_per_block)

		amount["P2PK"]["amount"] += btc_value
		amount["P2PK"]["total_output"] += n_output
		amount["P2PK"]["dust_output"] += n_dust_output
		amount["P2PK"]["dust_amount"] += total_btc_dust

	print "plotto P2PK" 	
	print P2PK_fee
	#bar_from_dict(P2PK_fee, "P2PK")

	P2PKH_fee = dict()
	P2PKH_block = dict()
	P2PKH_block_amount = dict()

	print "analizzo P2PKH"	
	for btc_value, n_output, n_dust_output, total_btc_dust, out_per_fee, out_per_block, amount_per_block in p.imap_unordered(P2PKH_amount, parsed_P2PKH_files_input):
		
		P2PKH_fee = merge_fee(P2PKH_fee, out_per_fee)
		P2PKH_block = merge_block_height(P2PKH_block, out_per_block)
		P2PKH_block_amount = merge_block_height(P2PKH_block_amount, amount_per_block)


		amount["P2PKH"]["amount"] += btc_value
		amount["P2PKH"]["total_output"] += n_output
		amount["P2PKH"]["dust_output"] += n_dust_output
		amount["P2PKH"]["dust_amount"] += total_btc_dust

	print "plotto P2PKH"	
	#bar_from_dict(P2PKH_fee, "P2PKH")
	
	P2MS_fee = dict()
	P2MS_block = dict()
	P2MS_block_amount = dict()
	print "analizzo P2MS"
	for btc_value, n_output, n_dust_output, total_btc_dust, out_per_fee, out_per_block, amount_per_block in p.imap_unordered(P2MS_amount, parsed_P2MS_files_input):
		
		P2MS_fee = merge_fee(P2MS_fee, out_per_fee)
		P2MS_block = merge_block_height(P2MS_block, out_per_block)
		P2MS_block_amount = merge_block_height(P2MS_block_amount, amount_per_block)

		amount["P2MS"]["amount"] += btc_value
		amount["P2MS"]["total_output"] += n_output
		amount["P2MS"]["dust_output"] += n_dust_output
		amount["P2MS"]["dust_amount"] += total_btc_dust


	print "plotto P2MS"		
	#bar_from_dict(P2MS_fee, "P2MS") 
	

	P2SH_fee = dict()
	P2SH_block = dict()
	P2SH_block_amount = dict()
	print "analizzo P2SH"
	for btc_value, n_output, n_dust_output, total_btc_dust, out_per_fee, out_per_block, amount_per_block in p.imap_unordered(P2SH_amount, parsed_P2SH_files_input):
		
		P2SH_fee = merge_fee(P2SH_fee, out_per_fee)
		P2SH_block = merge_block_height(P2SH_block, out_per_block)
		P2SH_block_amount = merge_block_height(P2SH_block_amount, amount_per_block)

		amount["P2SH"]["amount"] += btc_value
		amount["P2SH"]["total_output"] += n_output
		amount["P2SH"]["dust_output"] += n_dust_output
		amount["P2SH"]["dust_amount"] += total_btc_dust

	print "Plotto P2SH"
	#bar_from_dict(P2SH_fee, "P2SH")
	
	NSTD_fee = dict()
	NSTD_block = dict()
	NSTD_block_amount = dict()
	print "analizzo NSTD"
	for btc_value, n_output, n_dust_output, total_btc_dust, out_per_fee, out_per_block, amount_per_block in p.imap_unordered(NSTD_amount, parsed_NSTD_files_input):
		
		NSTD_fee = merge_fee(NSTD_fee, out_per_fee)
		NSTD_block = merge_block_height(NSTD_block, out_per_block)
		NSTD_block_amount = merge_block_height(NSTD_block_amount, amount_per_block)

		amount["NSTD"]["amount"] += btc_value
		amount["NSTD"]["total_output"] += n_output
		amount["NSTD"]["dust_output"] += n_dust_output
		amount["NSTD"]["dust_amount"] += total_btc_dust

	print "plotto NSTD"
	#bar_from_dict(NSTD_fee, "NSTD") 
	
	
	amount["Total"]["amount"] = amount["P2PK"]["amount"] + amount["P2PKH"]["amount"] + amount["P2MS"]["amount"] + amount["P2SH"]["amount"] + amount["NSTD"]["amount"]
	amount["Total"]["total_output"] = amount["P2PK"]["total_output"] + amount["P2PKH"]["total_output"] + amount["P2MS"]["total_output"] + amount["P2SH"]["total_output"] + amount["NSTD"]["total_output"]
	amount["Total"]["dust_output"] = amount["P2PK"]["dust_output"] + amount["P2PKH"]["dust_output"] + amount["P2MS"]["dust_output"] + amount["P2SH"]["dust_output"] + amount["NSTD"]["dust_output"]
	amount["Total"]["dust_amount"] = amount["P2PK"]["dust_amount"] + amount["P2PKH"]["dust_amount"] + amount["P2MS"]["dust_amount"] + amount["P2SH"]["dust_amount"] + amount["NSTD"]["dust_amount"]

	with open(CFG.parallel_data_path + "analysis/" + "tmp_amount.pkl", 'wb') as ser_file:
		pickle.dump(amount, ser_file)

	with open(CFG.parallel_data_path + "analysis/" + "tmp_P2PK.pkl", 'wb') as ser_file:
		pickle.dump(P2PK_fee, ser_file)

	with open(CFG.parallel_data_path + "analysis/" + "tmp_P2PKH.pkl", 'wb') as ser_file:
		pickle.dump(P2PKH_fee, ser_file)

	with open(CFG.parallel_data_path + "analysis/" + "tmp_P2MS.pkl", 'wb') as ser_file:
		pickle.dump(P2MS_fee, ser_file)

	with open(CFG.parallel_data_path + "analysis/" + "tmp_P2SH.pkl", 'wb') as ser_file:
		pickle.dump(P2SH_fee, ser_file)

	with open(CFG.parallel_data_path + "analysis/" + "tmp_NSTD.pkl", 'wb') as ser_file:
		pickle.dump(NSTD_fee, ser_file)

	with open(CFG.parallel_data_path + "analysis/" + "tmp_P2PK_block.pkl", 'wb') as ser_file:
		pickle.dump(P2PK_block, ser_file)

	with open(CFG.parallel_data_path + "analysis/" + "tmp_P2PKH_block.pkl", 'wb') as ser_file:
		pickle.dump(P2PKH_block, ser_file)

	with open(CFG.parallel_data_path + "analysis/" + "tmp_P2MS_block.pkl", 'wb') as ser_file:
		pickle.dump(P2MS_block, ser_file)

	with open(CFG.parallel_data_path + "analysis/" + "tmp_P2SH_block.pkl", 'wb') as ser_file:
		pickle.dump(P2SH_block, ser_file)

	with open(CFG.parallel_data_path + "analysis/" + "tmp_NSTD_block.pkl", 'wb') as ser_file:
		pickle.dump(NSTD_block, ser_file)

	with open(CFG.parallel_data_path + "analysis/" + "tmp_P2PK_block_amount.pkl", 'wb') as ser_file:
		pickle.dump(P2PK_block_amount, ser_file)

	with open(CFG.parallel_data_path + "analysis/" + "tmp_P2PKH_block_amount.pkl", 'wb') as ser_file:
		pickle.dump(P2PKH_block_amount, ser_file)

	with open(CFG.parallel_data_path + "analysis/" + "tmp_P2MS_block_amount.pkl", 'wb') as ser_file:
		pickle.dump(P2MS_block_amount, ser_file)

	with open(CFG.parallel_data_path + "analysis/" + "tmp_P2SH_block_amount.pkl", 'wb') as ser_file:
		pickle.dump(P2SH_block_amount, ser_file)

	with open(CFG.parallel_data_path + "analysis/" + "tmp_NSTD_block_amount.pkl", 'wb') as ser_file:
		pickle.dump(NSTD_block_amount, ser_file)
	
	return amount

def compare_dict_fee(dict1, dict2):

	for key in dict2:

		tmp_entry = dict2[key]

		if key in dict1:

			dict1[key]["amount"] += tmp_entry["amount"]
			#dict1[key]["n_output"] += tmp_entry["n_output"]
		else:

			dict1.update({key: tmp_entry})

	return dict1 

def merge_dict_fee(dict_list):

	main_dictionary = dict_list[0]
	del dict_list[0]

	for dictio in dict_list:

		main_dictionary = compare_dict_fee(main_dictionary, dictio)

	return main_dictionary

def compute_fee(file, payment):


	fee_dictionary = dict()

	for fee_per_byte in range(CFG.MIN_FEE_PER_BYTE, CFG.MAX_FEE_PER_BYTE, 10):

		print("considero ", fee_per_byte)

		fee_dictionary.update({fee_per_byte: {"amount": 0, "n_output": 0}})
		fin  = open(CFG.parallel_data_path + payment + "/" + file, 'r')

		for line in fin:

			data = ujson.loads(line[:-1])
			amount = data["amount"]
			dust = data["dust"]

			if dust > fee_per_byte:

				fee_dictionary[fee_per_byte]["amount"] += amount
				fee_dictionary[fee_per_byte]["n_output"] += 1

	print(fee_dictionary)
				
	return fee_dictionary


def compute_costi(file, payment):


	fee_dictionary = dict()

	for fee_per_byte in range(CFG.MIN_FEE_PER_BYTE, CFG.MAX_FEE_PER_BYTE, 10):

		#print("considero ", fee_per_byte)

		fee_dictionary.update({fee_per_byte: {"amount": 0}})
		fin  = open(CFG.parallel_data_path + payment + "/" + file, 'r')

		for line in fin:

			data = ujson.loads(line[:-1])

			dust = data["dust"]

			if dust > fee_per_byte:

				tx_lenght = data["tx_lenght"]
	
				costo = fee_per_byte * tx_lenght	
	
				fee_dictionary[fee_per_byte]["amount"] += costo
	
	print(fee_dictionary)
				
	return fee_dictionary



def compute_tot_costi(payment, serialize = True):

	file_list = os.listdir(CFG.parallel_data_path + payment + "/")

	p = Pool(CFG.POOL_NUMBER)

	compute_fee_part = partial(compute_costi, payment=str(payment))

	dict_list = []
	print("Partono i processi")


	for dictionary in p.imap_unordered(compute_fee_part, file_list):
		dict_list += [dictionary]

	main_dict = merge_dict_fee(dict_list)	

	if serialize:
		print("serializzo")

		with open(CFG.parallel_data_path + "analysis/" + "tmp_" + payment + "_costo_2.pkl", 'wb') as ser_file:
			pickle.dump(main_dict, ser_file)

	return main_dict 




	

def compute_tot_fee(payment, serialize = False):

	file_list = os.listdir(CFG.parallel_data_path + payment + "/")

	print(len(file_list))

	p = Pool(CFG.POOL_NUMBER)

	compute_fee_part = partial(compute_fee, payment=str(payment))

	dict_list = []
	print("Partono i processi")


	for dictionary in p.imap_unordered(compute_fee_part, file_list):
		dict_list += [dictionary]

	main_dict = merge_dict_fee(dict_list)		

	if serialize:
		print("serializzo")

		with open(CFG.parallel_data_path + "analysis/" + "tmp_" + payment + "_fee.pkl", 'wb') as ser_file:
			pickle.dump(main_dict, ser_file)

	return main_dict 




