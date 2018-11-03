from my_utxo_anal.statistics import *
from my_utxo_anal.parse_utxo import *
from my_utxo_anal import CFG
import datetime

date = datetime.datetime.now()
parse_ldb_par(CFG.POOL_NUMBER)

dump_par(CFG.POOL_NUMBER)

amount = compute_stat()

print "Chainstate repartition at: ", date
print_stat(amount)

