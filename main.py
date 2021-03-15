from crudpandas import PandasDb
if __name__ == "__main__":
  db = PandasDb()
  db.pickle('db/base_nova.df')
  print(db.select('descricao', 'Abacate').select('dsreduzida','ABACATE',exact=True).get('df'))
  