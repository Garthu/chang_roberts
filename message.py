class Message:
    def __init__(self, code = "", greatest_process = -1, sender = -1):
        self.code = code
        self.greatest_process = greatest_process
        self.sender = sender

    def set(self, code = None, greatest_process = None, sender = None):
        if (code):
            self.code = code
        if (greatest_process):
            self.greatest_process = greatest_process
        if (sender):
            self.sender = sender


    def encode(self):
        return f'{self.code}#{self.greatest_process}#{self.sender}'.encode()

    def decode(self, message):
        message = message.decode()
        if (message == ''):
            return
        code, greatest_process, sender = message.split("#")
        self.code = code
        self.greatest_process = int(greatest_process)
        self.sender = int(sender)