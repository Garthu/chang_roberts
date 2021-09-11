from dataclasses import dataclass

@dataclass
class Message:
    """Structure for organizing message data"""
    code: str
    greateste_process: int
    sender: int
