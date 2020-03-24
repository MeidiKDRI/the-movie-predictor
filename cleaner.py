class Cleaner:
    
    def __init__(self):
        pass
    
    def spliter(self, data, col):
        data[col] = data[col].split(",")
        return data
        
    def to_integer(self, data, col):

        if data[col] != "\\N":
            data[col] = int(data[col])
        return data
    
    def replace_nan(self, data, col) :
        if data[col] == '\\N' :
            data[col] = data.update('')
        return data
    
    def tconst_to_id(self, data) :
        data['_id'] = data['tconst']
        del data['tconst']
        return data