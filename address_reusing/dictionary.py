import pickle
import ujson
from my_utxo_anal import CFG
import os
from multiprocessing import Pool
from bitcoin_tools.wallet import hash_160_to_btc_address

def merge_dictionaries(serialize = False):

	dictionaries = os.listdir(CFG.data_path + "dictionaries/")

	res = dict()

	for f_name in dictionaries:

		dict_in = open(CFG.data_path + "dictionaries/" + f_name, 'r')
		temp = pickle.load(dict_in)
		res = {**res, **temp}
		dict_in.close()

	if serialize:

		pass
	else:

		return res







def init_dict_from_file(file_name):

	number = os.getpid()

	fin = open(CFG.data_path + "P2PKH/" + file_name, 'r')

	my_dict = dict()

	for line in fin:
		
		data = ujson.loads(line[:-1])
		
		script = data["script"]
		
		address = hash_160_to_btc_address(script, 0)

		my_dict.update({address : None})
		
	with open(CFG.data_path + "dictionaries/" + str(number)+ "_reusing.pkl", 'wb') as ser_file:

		pickle.dump(my_dict, ser_file)



def par_init_dict_from_file():

	files = os.listdir(CFG.data_path + "P2PKH")

	p = Pool(CFG.POOL_NUMBER)

	for x in p.map(init_dict_from_file, files):
		pass

	p.close()
	p.join()
