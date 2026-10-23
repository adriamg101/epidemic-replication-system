class Utils:
    def __init__(self, verbose, name):
        self.verbose = verbose
        self.name = name

    def comment(self, str):
        # SI no hi ha flag de verbose no comentem res
        if not self.verbose: return
        print(self.name + ": " + str)
