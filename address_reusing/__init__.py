try:
    import address_reusing.conf as CFG
except ImportError:
    raise Exception("You don't have a configuration file. Make a copy of conf.py")
