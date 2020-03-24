import csv

class TSV:
    def __init__(self, filepath):
        # TODO check if the file exists
        self.fh = open(filepath, 'rt', encoding = 'utf-8')
        self.header = self.fh.readline()
        # TODO What if there is no header in the file?

    def read_sequential(self, number_of_lines):
        lines = []
        counter = 0
        line = self.fh.readline()
        while line and counter < number_of_lines:
            lines.append(line)
            counter += 1
            line = self.fh.readline()

        # Return None if lines is empty
        if len(lines) == 0:
            return None

        lines.insert(0, self.header)
        # delimiter = '\t' for a file.tsv
        lines_as_dict = csv.DictReader(lines, delimiter='\t', quoting = csv.QUOTE_NONE)

        return lines_as_dict