import pandas as pd

class PandasDb(object):
  def __init__(self):
    self.df = pd.DataFrame()
    self.db = pd.DataFrame()
    self.name = 'new.df'
    self.tp = 'pickle'
    self.compression = 'infer'
    self.engine = 'openpyxl'
    self.encoding = 'utf-8'
    self.separator = ';'

  # MÉTODOS DE ABERTURA DE ARQUIVO DE BANCO
  def pickle(self, filelocation, compression='infer'):
    '''Ler arquivos do tipo pickle 
       filelocation=localização do arquivo;
       compression=Tipo de compressão (infer padrão)'''
    try:
      self.df = pd.read_pickle(filelocation, compression)
      self.db = self.df.copy()
      self.tp = 'pickle'
      self.name = filelocation
      self.compression = compression
      return self
    except Exception as e:
      raise(e)

  def excel(self, filelocation, engine='openpyxl'):
    '''Ler arquivos do tipo xlsx/xls 
       filelocation=localização do arquivo;'''
    try:
      self.df = pd.read_excel(filelocation, engine=engine)
      self.db = self.df.copy()
      self.tp = 'excel'
      self.name = filelocation
      self.engine = engine
      return self
    except Exception as e:
      raise(e)

  def csv(self, filelocation, separator = ";" ,encoding='utf-8'):
    '''Ler arquivos do tipo csv 
       filelocation=localização do arquivo;
       separator=separador de colunas;
       encoding=codificação do arquivo;
    '''
    try:
      self.df = pd.read_csv(filelocation, sep=separator, encoding=encoding)
      self.db = self.df.copy()
      self.tp = 'csv'
      self.name = filelocation
      self.encoding = encoding
      self.separator = separator
      return self
    except Exception as e:
      raise(e)

  # MÉTODOS DE CONSULTA DO BANCO
  def select(self, column=None, contain=None, exact=False):
    '''Seleciona informações no dataframe
       column=nome da coluna (None para todas);
       contain=Valor contido na coluna;
    '''
    try:
      if column and contain:
        if exact:
          self.df = self.df[self.df[column] == contain] 
        else:
          self.df = self.df[self.df[column].str.contains(contain)] 
      elif column:
        self.df = self.df[column] 
      return self
    except Exception as e:
      raise(e)
  
  def get_index(self, index):
    '''Seleciona informações no dataframe e retorna
       index=valor do index 
    '''
    try:
        return self.df.loc[index]
    except Exception as e:
      raise(e)
  
  def get(self, typeReturn='df', criter = 'index'):
    '''Recupera o valor do banco de dados filtrado'''
    try:
      if typeReturn=='dict':
        return self.df.to_dict(criter)
      elif typeReturn=='list':
        return list(self.df.tolist())
      else:
        return self.df
    except Exception as e:
      raise(e)

  def reset(self):
    '''Restaura o banco'''
    try:
      self.df = self.db.copy()
      return self
    except Exception as e:
      raise(e)

  # METODOS DE MANIPULAÇÃO DO BANCO
  def insert(self, values):
    '''Insere um dicionário ao banco de dados'''
    try:
      d = pd.DataFrame([values])
      self.db = pd.concat([self.db, d]).reset_index(drop=True)
      self.df = self.db.copy()
      return self
    except Exception as e:
      raise(e)
  
  def update(self, index, values):
    '''Edita um campo do banco com um dicionário'''
    try:
      d = pd.Series(values)
      self.db.loc[index] = d
      self.df = self.db.copy()
      return self
    except Exception as e:
      raise(e)

  def save(self):
    '''Salva as modificações no banco'''
    try:
      if self.tp=='pickle':
        self.db.to_pickle(self.name, self.compression)
      elif self.tp=='excel':
        self.db.to_excel(self.name)
      elif self.tp=='csv':
        self.db.to_csv(self.name, encoding=self.encoding, sep=self.separator)
      return self
    except Exception as e:
      raise(e)