try:
    import my_utxo_anal.conf as CFG
except ImportError:
    raise Exception("You don't have a configuration file. Make a copy of conf.py")


