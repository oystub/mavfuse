from sortedcontainers import SortedDict


class ReceivedBurstData:
    def __init__(self, offset, total_size):
        self._initial_offset = offset
        self._initial_size = total_size
        self._total_size = total_size
        self._data = bytearray()

        if self._total_size == 0:
            self.missing_ranges = SortedDict()
        else:
            self.missing_ranges = SortedDict({offset: self._total_size})

    def mark_completed(self, offset, size, data=None):
        start = offset
        end = offset + size

        new_ranges = SortedDict()
        overlapping_keys = []

        for start_range, range_size in self.missing_ranges.items():
            end_range = start_range + range_size

            if end_range <= start:
                continue
            if start_range >= end:
                break

            if start_range < start:
                new_ranges[start_range] = start - start_range
            if end_range > end:
                new_ranges[end] = end_range - end

            overlapping_keys.append(start_range)

        for key in overlapping_keys:
            del self.missing_ranges[key]

        self.missing_ranges.update(new_ranges)

        if data:
            if len(self._data) < end:
                self._data.extend(bytearray(end - len(self._data)))
            self._data[start-self._initial_offset:end-self._initial_offset] = data

    def eof(self, offset):
        self.mark_completed(offset, self._total_size-(offset-self._initial_offset))
        self._total_size = offset-self._initial_offset

    def get_missing(self):
        return [(start, size) for start, size in self.missing_ranges.items()]

    def get_next_missing(self):
        if not self.missing_ranges:
            return None
        start, size = self.missing_ranges.peekitem(0)
        return (start, size)

    def get_bytes_missing(self):
        return sum(size for start, size in self.missing_ranges.items())

    def get_data(self):
        return self._data[:self._total_size]
