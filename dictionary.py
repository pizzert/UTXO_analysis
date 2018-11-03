import cPickle as pickle
import ujson
from my_utxo_anal import CFG
import os
from multiprocessing import Pool
from bitcoin_tools.wallet import hash_160_to_btc_address



def init_dict_from_file(file_name):

	number = os.getpid()

	fin = open(CFG.parallel_data_path + "P2PKH/" + file_name, 'r')

	my_dict = dict()

	for line in fin:
		
		data = ujson.loads(line[:-1])
		amount = data["amount"]

		script = data["script"]
		
		address = hash_160_to_btc_address(script, 0)

		if address in my_dict:

			print("aumento indirizzo")

			my_dict[address]["n_output"] += 1
			my_dict[address]["amount"] += amount

		else:	

			print("scrivo nel dizionario")

			my_dict.update({address : {"amount": amount, "n_output": 1, "reused": 0, "block": None, "tx_id": None}})
				
	with open(CFG.parallel_data_path + "dictionaries/" + str(number)+ "_reusing.pkl", 'wb') as ser_file:

		pickle.dump(my_dict, ser_file)



def par_init_dict_from_file():

	files = os.listdir(CFG.parallel_data_path + "P2PKH")

	p = Pool(CFG.POOL_NUMBER)

	for x in p.map(init_dict_from_file, files):
		pass

	p.close()
	p.join()











 



















"""

array = {"key1":0, "key2":1, "key3":0, "key4":1}

file = open('array.pkl', 'wb')

pickle.dump(array, file)


file.close()


file = open('array.pkl', 'rb')

data = pickle.load(file)

print data['key1']

"""
