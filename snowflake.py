import time
import threading

class SnowflakeIDGenerator:
    def __init__(self, machine_id: int, epoch: int = 1609459200000):
        # Epoch time to start counting from (e.g., Jan 1, 2021)
        self.epoch = epoch
        self.machine_id = machine_id
        self.sequence = 0
        self.last_timestamp = -1
        
        # Constants
        self.machine_id_bits = 10  # 10 bits for machine ID (1024 machines)
        self.sequence_bits = 12    # 12 bits for sequence number (4096 IDs per millisecond)
        self.machine_id_shift = self.sequence_bits  # Shift for machine ID
        self.timestamp_shift = self.sequence_bits + self.machine_id_bits  # Shift for timestamp

        # Maximum values for sequence number and machine ID
        self.max_machine_id = (1 << self.machine_id_bits) - 1
        self.max_sequence = (1 << self.sequence_bits) - 1

        # Mutex to ensure thread safety
        self.lock = threading.Lock()

    def _current_timestamp(self):
        """Get current timestamp in milliseconds"""
        return int(time.time() * 1000)

    def _wait_for_next_millisecond(self, last_timestamp):
        """Wait for the next millisecond to avoid duplicate IDs"""
        timestamp = self._current_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._current_timestamp()
        return timestamp

    def generate_id(self):
        """Generate a unique ID"""
        with self.lock:
            timestamp = self._current_timestamp()

            # Check if we are in the same millisecond as the last ID generated
            if timestamp == self.last_timestamp:
                self.sequence = (self.sequence + 1) & self.max_sequence
                if self.sequence == 0:
                    timestamp = self._wait_for_next_millisecond(self.last_timestamp)
            else:
                self.sequence = 0

            self.last_timestamp = timestamp

            # Shift and combine the parts to form the final ID
            id = ((timestamp - self.epoch) << self.timestamp_shift) | (self.machine_id << self.machine_id_shift) | self.sequence
            return id


# Example usage:
if __name__ == "__main__":
    machine_id = 1  # Unique machine ID (could be anything, ensure it's unique across machines)
    generator = SnowflakeIDGenerator(machine_id)

    # Generate 5 IDs
    for _ in range(5):
        print(generator.generate_id())
