import os
import ujson
import plyvel
from my_utxo_anal import CFG
from binascii import hexlify, unhexlify
from multiprocessing import Pool
from binascii import hexlify, unhexlify
from bitcoin_tools.analysis.status.utils import roundup_rate, parse_ldb, check_multisig, deobfuscate_value,get_serialized_size, decode_utxo, get_min_input_size
from functools import partial

def extract_values(line, o_key):

	file_number = str(os.getpid())
        
        fout = open(CFG.parallel_data_path + "Parsed_UTXO/" + file_number + "_par_UTXO.json", 'a')
	
	serialized_length = len(line[0]) + len(line[1])
		
	key = hexlify(line[0])


	hex_line = hexlify(line[1])
		
	if o_key is not None:
		hex_line = hexlify(line[1])
		value = deobfuscate_value(o_key, hex_line)
	else:
		value = hexlify(line[1])
    
    	value = decode_utxo(value, key, 0.15)
        
        fout.write(ujson.dumps({"key": key[2:], "value": value, "len": serialized_length}, sort_keys=True) + "\n")

	fout.close()

def parse_ldb_par(processes=2, db_name='chainstate'):
	
	print "Open the database"
	db = plyvel.DB(CFG.btc_core_path + "chainstate", compression=None)
	
	print "Define an iterator"
	iter_chain = db.iterator(prefix=b'C')

	print "Prendo la chiave di deoffuscazione"
	o_key = db.get((unhexlify("0e00") + "obfuscate_key"))

	if o_key is not None:
		o_key = hexlify(o_key)[2:]

	print "Calcolo la funzione parziale"
	par_extract_values = partial(extract_values, o_key = o_key)

	data_folder = CFG.parallel_data_path + "Parsed_UTXO/"
        
        print "Controllo che ", data_folder, " sia vuota"
	if os.listdir(data_folder) != []:
          print "Non e' vuota, quindi pulisco"
          map(os.unlink, (os.path.join(data_folder,f) for f in os.listdir(data_folder)))

	print "Definisco i pool e faccio partire i processi"
	p = Pool(processes)
	
	#for number in p.imap_unordered(par_extract_values, iter_chain):
	#	pass
	
	for number in p.map(par_extract_values, iter_chain):
		pass

	p.close()
	p.join()

	db.close()

def dump(fin_name):

        number = os.getpid()
        print "Apro i file in Parsed"
	fin = open(CFG.parallel_data_path + "Parsed_UTXO/" + fin_name, 'r')

	folder_pk   = CFG.parallel_data_path + "P2PK/" 
	folder_pkh  = CFG.parallel_data_path + "P2PKH/"
	folder_p2ms = CFG.parallel_data_path + "P2MS/"   
	folder_p2sh = CFG.parallel_data_path + "P2SH/"  
	folder_nstd = CFG.parallel_data_path + "NSTD/"  
        
        print "Apro i file degli output"
	fout_pk   = open(folder_pk   + str(number) +	"_f_parsed_std_pk.json", 'a')
	fout_pkh  = open(folder_pkh  + str(number) +	"_f_parsed_std_P2PKH.json", 'a')
	fout_p2ms = open(folder_p2ms + str(number) +	"_f_parsed_std_P2MS.json", 'a')
	fout_p2sh = open(folder_p2sh + str(number) +	"_f_parsed_std_P2SH.json", 'a')
	fout_nstd = open(folder_nstd + str(number) +	"_f_parsed_nstd.json", 'a')

	for line in fin:
		
		data = ujson.loads(line[:-1])
		utxo = data['value']
		
		for out in utxo.get("outs"):

			amount = utxo.get("outs")[0]["amount"]
			out_type = utxo.get("outs")[0]["out_type"]
			script = utxo.get("outs")[0]["data"]

			min_size = get_min_input_size(out, utxo["height"], count_p2sh=True)
			
			dust = 0
			
			out_size = 0
			if out_type in [2,3,4,5]:
				out_size = 33
			elif check_multisig(script, std=True):
				out_size = 31

			tot_size = out_size + min_size

			raw_dust = amount/float(out_size + min_size)

			dust = roundup_rate(raw_dust, CFG.FEE_STEP)		
			
	
			result = {"amount": amount,
													"tx_height": utxo["height"],
													"dust": dust,
	            "out_type": out_type,
													"script": script,
													"tx_lenght": tot_size}
	
			if out_type == 0:
				fout_pkh.write(ujson.dumps(result) + '\n')
			elif out_type == 1:
				fout_p2sh.write(ujson.dumps(result) + '\n')
			elif out_type in [2, 3, 4, 5]:
				fout_pk.write(ujson.dumps(result) + '\n')
			elif check_multisig(script, std=True):
				fout_p2ms.write(ujson.dumps(result) + '\n')
			else:
				fout_nstd.write(ujson.dumps(result) + '\n')

def dump_par(processes):

 data_folder = CFG.parallel_data_path + "Parsed_UTXO/"
        
 folder_pk   = CFG.parallel_data_path + "P2PK/" 
 folder_pkh  = CFG.parallel_data_path + "P2PKH/"
 folder_p2ms = CFG.parallel_data_path + "P2MS/"   
 folder_p2sh = CFG.parallel_data_path + "P2SH/"  
 folder_nstd = CFG.parallel_data_path + "NSTD/" 

 if os.listdir(folder_pk) != []:
  map(os.unlink, (os.path.join(folder_pk,f) for f in os.listdir(folder_pk)))

 if os.listdir(folder_pkh) != []:
  map(os.unlink, (os.path.join(folder_pkh,f) for f in os.listdir(folder_pkh)))

 if os.listdir(folder_p2ms) != []:
  map(os.unlink, (os.path.join(folder_p2ms,f) for f in os.listdir(folder_p2ms)))

 if os.listdir(folder_p2sh) != []:
  map(os.unlink, (os.path.join(folder_p2sh,f) for f in os.listdir(folder_p2sh)))

 if os.listdir(folder_nstd) != []:
  map(os.unlink, (os.path.join(folder_nstd,f) for f in os.listdir(folder_nstd)))


        
 file_input = os.listdir(data_folder)
 p = Pool(processes)

 for x in p.imap_unordered(dump, file_input):
  print "Done"
