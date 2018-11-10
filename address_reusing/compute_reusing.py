from blockchain_parser.blockchain import Blockchain
from blockchain_parser.script import is_public_key
from blockchain_parser.address import Address
from multiprocessing import Pool
import address_reusing.conf as CFG
import blockchain
import datetime
import pickle 
import time
import os


def compare_dictionary(main, temp):

	for key in temp:
		
		tmp_entry = temp[key]

		if key in main:

			print(key + "è in main")
			main[key]["amount"] += tmp_entry["amount"]
			main[key]["n_output"] += tmp_entry["n_output"]
		else:
			print(key + "non è in main")
			main.update({key: tmp_entry})

	return main




def merge_dictionaries(serialize = True):

	dictionaries = os.listdir(CFG.data_path + "dictionaries/")

	print("Carico il main")
	serialized_file = open(CFG.data_path + "dictionaries/" + dictionaries[0], 'rb')
	main_dictionary = pickle.load(serialized_file)
	del dictionaries[0]

	print("Passo in rassegna tutto")
	for f_name in dictionaries:

		with open(CFG.data_path + "dictionaries/" + f_name, 'rb') as dict_file:

			print("apro il dizionarietto")
			tmp_dict = pickle.load(dict_file)

		print("comparo i dizionari")
		main_dictionary = compare_dictionary(main_dictionary, tmp_dict)

	if serialize:

		print("serializzo tutto")
		with open(CFG.data_path + "tmp_reusing.pkl", 'wb') as ser_file:

			pickle.dump(main_dictionary ,ser_file)


	return main_dictionary 

def is_p2pkh_scriptsig(data):

	data_length = len(data)
	pk = data[-33:]
	if (data_length in range(100, 111) or data_length in range(130, 137)) and is_public_key(pk):
		return True, pk

	return False, None

def resolve_address(data):

	address = Address.from_public_key(data)
	return address.address

def parse_unordered():

	file = "/home/gino/Desktop/blocks"

	blockchain = Blockchain(file)

	for block in blockchain.get_unordered_blocks():
		for tx in block.transactions:
			tx_id = tx.txid
			for no, Input in enumerate(tx.inputs):
				raw_script = Input.script.hex
				check, pk = is_p2pkh_scriptsig(raw_script)
				if check:
					address = resolve_address(pk)
					if address in address_dict and address_dict[address] is None:		
							print("Scrivo transazione: ",tx_id ," nel dizionario\n")
							address_dict[address] = {"height": block_height, "tx_id": tx_id}

def popolate_dict():


	if os.path.isfile(CFG.data_path + "tmp_reusing.pkl"):
		print("Carico il dizionario")
		file = open(CFG.data_path + "tmp_reusing.pkl", 'rb')
		address_dict = pickle.load(file)
		file.close()
	else:
		print("Creo il dizionario")
		address_dict = merge_dictionaries(serialize = True)


	blockchain = Blockchain(CFG.btc_core_path + "blocks/")

	if os.path.isfile(CFG.data_path  + "tmp_block.pkl"):

		with open(CFG.data_path  + "tmp_block.pkl", 'rb') as ser_file:

			start = pickle.load(ser_file)

	else:

		start = 0



	print("Parto da: ", start)
	print("Inizio analisi")

	offset = 0

	try:
		for block in blockchain.get_ordered_blocks(CFG.btc_core_path + "blocks/index", start=start, cache=CFG.data_path + "blockchain_cache.pkl"):
			
			offset += 1
			block_height = block.height
			
			ts = time.time()
			st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
			print(st + ": Blocco n: " + str(block_height) + "\n")
			with open(CFG.data_path + "log.txt", 'a') as file:
				file.write(st + ": Blocco n: " + str(block_height) + "\n")

			#print(st, ": Blocco n: ", block_height)
			for tx in block.transactions:
			
				tx_id = tx.txid
				for no, Input in enumerate(tx.inputs):
					
					raw_script = Input.script.hex
					check, pk = is_p2pkh_scriptsig(raw_script)
							
					if check:
		
						address = resolve_address(pk)
						
						print("Cerco " + address)
						if address in address_dict and address_dict[address]["reused"] == 0:		

							print("Aggiungo " + address)
							with open(CFG.data_path + "log.txt", 'a') as file:
								file.write("Aggiungo " + address + ": " + str(block_height) + " tx: " + tx_id +  "\n")
								
							address_dict[address]["reused"] = 1
							address_dict[address]["block"] = block_height
							address_dict[address]["tx_id"] = tx_id
	
							if offset % 1000 == 0:

								with open(CFG.data_path + "tmp_reusing.pkl", 'wb') as ser_file:

									pickle.dump(address_dict, ser_file)

								with open(CFG.data_path + "tmp_block.pkl", 'wb') as ser_file:

									pickle.dump(block_height, ser_file)


	except KeyboardInterrupt:

		print("Scrivo le variabili al blocco: " + str(block_height))

		with open(CFG.data_path + "tmp_reusing.pkl", 'wb') as ser_file:

			pickle.dump(address_dict, ser_file)

		with open(CFG.data_path + "tmp_block.pkl", 'wb') as ser_file:

			pickle.dump(block_height, ser_file)



	with open(CFG.data_path + "tmp_reusing.pkl", 'wb') as ser_file:

		pickle.dump(address_dict, ser_file)

def parse_dictionary(dictionary):

	total = 0
	reused = 0

	print(len(dictionary))
	
	for k in dictionary:

		total += 1

		#print(k)

		if dictionary[k] is not None:
			reused += 1


		if (total%500 == 0):
			print("total: ", total) 
			print("reused: ", reused)
			
	return total, reused

def par_parse_dictionary():

	print("Carico dizionario")
	with open(CFG.data_path + "tmp_reusing.pkl", 'rb') as file:

		dictionary = pickle.load(file)

	"""	
	print("definisco i pool")
	p = Pool(1)

	total = 0
	reused = 0
	for x, y in p.imap_unordered(parse_dictionary, dictionary):
		
		total += total
		reused += reused
	"""
	parse_dictionary(dictionary)

#par_parse_dictionary()

		



	
