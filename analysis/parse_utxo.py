from utils import roundup_rate, check_multisig, deobfuscate_value, decode_utxo, get_min_input_size
from binascii import hexlify, unhexlify
from multiprocessing import Pool
from functools import partial
import conf as CFG
import plyvel
import ujson
import os

def extract_values(line, o_key):

    """
    Extract the value of the UTXO from the levelDB iterator and then stores it in a file

    :param line: Entry of the database Iterator
    :type line: tuple
    :param o_key: obfuscation key
    :type o_key: bytes
    :return: None
    :rtype: None

    """

    #Create the file in which the entry will be stored. Since many instances of this function are run,
    # a number is assigned in the filename in order to make different file.
    #This will create a number of file equal to the value of POOL_NUMBER.
    file_number = str(os.getpid())
    fout = open(CFG.data_path + "parsed_UTXO" + file_number + "_par_UTXO.json", 'a')

    key = hexlify(line[0])

    #if the obfuscation key is used, the entry is de obfuscated and then decodified
    if o_key is not None:
        value = deobfuscate_value(o_key, hexlify(line[1]))
    else:
        value = hexlify(line[1])

    value = decode_utxo(value, key, 0.15)

    #The decoded output is written in the file in JSON format
    fout.write(ujson.dumps({"key": key[2:], "value": value}, sort_keys=True) + "\n")

    fout.close()

def parse_ldb(processes=CFG.POOL_NUMBER, db_name='chainstate'):

    """
    Open LevelDB with plyvel and stores the entries in a file.
    WARNING: In order to make this analysis faster, an object iterator that represent
    the entries of the chainstate is used. This will take a lot of memory

    :param processes: number of processes used, by default POOL_NUMBER from conf.py is used
    :param db_name: Name of the folder in which the chainstate is stored
    :return:None
    """

    print("Opening the DataBase")
    #LevelDB is open with plyvel. then an iterator is defined
    db = plyvel.DB(CFG.btc_core_path + db_name, compression=None)
    iter_chain = db.iterator(prefix=b'C')

    #Extract the obfuscation key
    o_key = db.get((unhexlify("0e00") + "obfuscate_key".encode('ascii')))
    if o_key is not None:
        o_key = hexlify(o_key)[2:]

    #If the file folder it's not empty, I erase data from older analysis
    print("Checking folder")
    data_folder = CFG.data_path + "Parsed_UTXO/"

    if os.listdir(data_folder) != []:

        map(os.unlink, (os.path.join(data_folder, f) for f in os.listdir(data_folder)))

    #Processes are defined and launched
    partial_extract_values = partial(extract_values, o_key = o_key)
    print("Starting processes")
    p = Pool(processes)

    for number in p.map(partial_extract_values, iter_chain):
        pass

    p.close()
    p.join()

    db.close()

def dump(fin_name):

    number = os.getpid()
    print("Opening file in Parsed_UTXO")
    fin = open(CFG.data_path + "Parsed_UTXO" + fin_name, 'r')

    folder_pk = CFG.data_path + "P2PK/"
    folder_pkh = CFG.data_path + "P2PKH/"
    folder_p2ms = CFG.data_path + "P2MS/"
    folder_p2sh = CFG.data_path + "P2SH/"
    folder_nstd = CFG.data_path + "NSTD/"
    fout_pk = open(folder_pk + str(number) + "_f_parsed_std_pk.json", 'a')
    fout_pkh = open(folder_pkh + str(number) + "_f_parsed_std_P2PKH.json", 'a')
    fout_p2ms = open(folder_p2ms + str(number) + "_f_parsed_std_P2MS.json", 'a')
    fout_p2sh = open(folder_p2sh + str(number) + "_f_parsed_std_P2SH.json", 'a')
    fout_nstd = open(folder_nstd + str(number) + "_f_parsed_nstd.json", 'a')

    for line in fin:

        data = ujson.loads(line[:-1])
        utxo = data['value']

        for out in utxo.get("outs"):

            amount = utxo.get("outs")[0]["amount"]
            out_type = utxo.get("out")[0]["out_type"]
            script = utxo.get("outs")[0]["data"]

            min_size = get_min_input_size(out, utxo["height"], count_p2sh= True)

            dust = 0

            out_size = 0
            if out_type in [2,3,4,5]:
                out_size = 33
            elif check_multisig(script, std=True):
                out_size = 31

            tot_size = out_size + min_size
            raw_dust = amount/float(out_size + min_size)
            dust = roundup_rate(raw_dust, CFG.FEE_STEP)

            result = {"amount": amount, "tx_height": utxo["height"], "dust": dust,
                      "out_type": out_type,	"script": script, "tx_lenght": tot_size}

            if out_type == 0:
                fout_pkh.write(ujson.dumps(result) + '\n')
            elif out_type == 1:
                fout_p2sh.write(ujson.dumps(result) + '\n')
            elif out_type in [2,3,4,5]:
                fout_pk.write(ujson.dumps(result) + '\n')
            elif check_multisig(script, std=True):
                fout_p2ms.write(ujson.dumps(result) + '\n')
            else:
                fout_nstd.write(ujson.dumps(result) + '\n')

def dump_par(processes):

    data_folder = CFG.data_path + "Parsed_UTXO/"
    folder_pk = CFG.data_path + "P2PK/"
    folder_pkh = CFG.data_path + "P2PKH/"
    folder_p2ms = CFG.data_path + "P2MS/"
    folder_p2sh = CFG.data_path + "P2SH/"
    folder_nstd = CFG.data_path + "NSTD/"

    if os.listdir(folder_pk) != []:
        map(os.unlink, (os.path.join(folder_pk, f) for f in os.listdir(folder_pk)))
    if os.listdir(folder_pkh) != []:
        map(os.unlink, (os.path.join(folder_pkh, f) for f in os.listdir(folder_pkh)))
    if os.listdir(folder_p2ms) != []:
        map(os.unlink, (os.path.join(folder_p2ms, f) for f in os.listdir(folder_p2ms)))
    if os.listdir(folder_p2sh) != []:
        map(os.unlink, (os.path.join(folder_p2sh, f) for f in os.listdir(folder_p2sh)))
    if os.listdir(folder_nstd) != []:
        map(os.unlink, (os.path.join(folder_nstd, f) for f in os.listdir(folder_nstd)))

    file_input = os.listdir(data_folder)
    p = Pool(processes)

    for x in p.imap_unordered(dump, file_input):
        pass



