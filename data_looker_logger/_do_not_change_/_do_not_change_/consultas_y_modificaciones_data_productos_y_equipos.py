import pandas as pd
import numpy as np
import tabula
class CloseException(Exception):
  pass
from datetime import datetime
import pytz

"""### clases"""

class Looker_logger:
  '''
  Class to look and log recs, for a cellphones marketer.
  '''
  def __init__(self, file, consultas_path):
    '''
    Constructor method.
    Parameters:
      file: Name and path of the file where the data will be logged or modified.
    '''
    self.file = file
    self.consultas_path = consultas_path

  def datetime_arg(self):
    argentina_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
    current_time_argentina = datetime.now(argentina_timezone)
    return current_time_argentina

  def closer(self, message):
    res = input(f'{message}')
    if res == 'c':
      raise CloseException()
    else:
      return res

  def read_file(self):
    temp = pd.read_csv(self.file)
    for i in temp.columns:
      temp[i] = [e.strip() if type(e) == str else e for e in temp[i]]
    return temp

  def authorization(self, message):
    res = input(f'Quiere proceder con {message}? (y/n): ')
    if res == 'y':
      print(f'{message} autorizado.\n')
      return 1
    else:
      print(f'{message} denegado.\n')
      return 0

  def no_duplicates(self, data):
    temp = data.copy()
    duplicados = temp.duplicated(subset=['modelo','marca','producto','calidad','color'], keep='last')
    if duplicados.any():
      id_duplicados = temp[duplicados].id.to_list()
      print(f'Se encontraron duplicados en los id: {id_duplicados}')
      res = self.authorization('eliminar duplicados anteriores')
      if res == 1:
        temp = temp[~temp.id.isin(id_duplicados)]
        self.save_file(temp)
        print('Duplicados eliminados.\n')
      else:
        print('Se conservan duplicados.\n')

  def set_id(self):
    temp = self.read_file()
    last_id = temp.id.values[-1]
    new_id = int(last_id) + 1
    return new_id

  def save_file(self, data):
    temp = data.copy()
    temp.to_csv(self.file, index=False)
    print('Archivo actualizado.\n')

  def delete_rec(self, id):
    temp = self.read_file()
    temp = temp[temp.id != id]
    res = self.authorization(f'eliminar registro con id: {id}')
    if res == 1:
      self.save_file(temp)
    else:
      pass

  def make_rec(self, tipo='equipo'):
    temp = self.read_file()
    cols = temp.columns
    print('Agregue datos del nuevo registro (cancelar: "c").')
    for columna in cols:
      try:
        if columna == 'id':
          id = self.set_id()
          print(f'id: {id}')
          new_row = pd.DataFrame([{'id':id}])
        elif columna == 'fecha_reg':
          new_row.loc[new_row.id == id, 'fecha_reg'] = self.datetime_arg().date()
        elif columna == 'fecha_actualizacion':
          new_row.loc[new_row.id == id, 'fecha_actualizacion'] = self.datetime_arg().date()
        else:
          if tipo == 'equipo':
            if columna == 'producto':
              new_row.loc[new_row.id == id, 'producto'] = ''
            else:
              res = self.closer(f'{columna}: ')
              new_row.loc[new_row.id == id, columna] = res
          elif tipo == 'producto':
            if (columna == 'marca') | (columna == 'modelo'):
              new_row.loc[new_row.id == id, 'marca'] = ''
              new_row.loc[new_row.id == id, 'modelo'] = ''
            else:
              res = self.closer(f'{columna}: ')
              new_row.loc[new_row.id == id, columna] = res
      except CloseException:
        return 'Registro cancelado.\n'
    return new_row

  def add_rec(self, tipo='equipo'):
    temp = self.read_file()
    temp1 = self.make_rec(tipo=tipo)
    df = pd.concat([temp, temp1])
    self.save_file(df)

  def change_prod_info(self, id, column, value):
    temp = self.read_file()
    temp.loc[temp.id == id, column] = value
    temp.loc[temp.id == id, 'fecha_actualizacion'] = str(self.datetime_arg().date())
    self.save_file(temp)

  def consulta_fecha(self, begin='', end=''):
    if begin == '':
      begin = '2012-1-1'
    if end == '':
      end = str(self.datetime_arg().date())
    temp = self.read_file()
    temp['fecha_reg'] = pd.to_datetime(temp.fecha_reg)
    df = temp[(temp.fecha_reg >= begin) & (temp.fecha_reg <= end)]
    return df

  def consulta_producto(self, df):
    temp = df.copy()
    temp = temp[temp.producto.notna()]
    return temp

  def consulta_marca(self, df, marca=''):
    temp = df.copy()
    if marca == '':
      df = temp
    else:
      df = temp[temp.marca == marca]
    return df

  def consulta_modelo(self, df, modelo=''):
    temp = df.copy()
    if modelo == '':
      df = temp
    else:
      df = temp[temp.modelo == modelo]
    return df

  def consulta_agotados(self, df):
    temp = df.copy()
    temp = temp[temp.stock.notna()]
    return temp

  def consulta_no_agotados(self, df):
    temp = df.copy()
    temp = temp[temp.stock.isna()]
    return temp

  def retrieve_csv_file(self, data, titulo):
    tiempo = str(self.datetime_arg())[:18].replace(' ','_').replace(':','-')
    data.to_csv(f'{self.consultas_path}\_{titulo}_{tiempo}.csv', index=False)
    print('Consulta realizada, guardada en carpeta consultas.\n')

"""### functions"""

def decor(func):
  def wrapper(*args, **kwargs):
    print()
    func(*args, **kwargs)
    print()
  return wrapper

@decor
def consultas(archivo_principal, archivo_consulta):
  try:
    ll = Looker_logger(archivo_principal, archivo_consulta)
    fecha_inicio = ll.closer('Fecha de inicio de consulta (ej. 2023-1-1), deje el campo vacio para empezar desde 2012\n(introduzca "c" para cerrar): ')
    fecha_fin = ll.closer('Fecha de final de consulta (ej. 2023-10-31), deje el campo vacio para abarcar todos los registros\n(introduzca "c" para cerrar): ')
    temp = ll.consulta_fecha(fecha_inicio, fecha_fin)
    tipo = ll.closer('Quiere consultar un producto o un equipo (opciones: "producto", "equipo", deje vacio para consultar ambos)\n(introduzca "c" para cerrar): ')
    if tipo == 'equipo':
      marca = ll.closer('Ingrese el nombre de la marca a consultar, deje el campo vacio para consultar todas\n(introduzca "c" para cerrar): ')
      temp = ll.consulta_marca(temp, marca = marca)
      modelo = ll.closer('Ingrese el nombre del modelo a consultar, deje el campo vacio para consultar todas\n(introduzca "c" para cerrar): ')
      temp = ll.consulta_modelo(temp, modelo = modelo)
      titulo = 'equipo_'
    elif tipo == 'producto':
      temp = ll.consulta_producto(temp)
      titulo = 'producto_'
    else:
      titulo = 'general_'
      print('Consulta general.')
    stock = ll.closer('Quiere consultar agotados o no agotados (opciones: deje vacio para no agotados, "agotados")\n(introduzca "c" para cerrar): ')
    if stock == 'agotados':
      temp = ll.consulta_agotados(temp)
      titulo = titulo + 'agotados'
    else:
      temp = ll.consulta_no_agotados(temp)
      titulo = titulo + 'no_agotados'
    ll.retrieve_csv_file(temp, titulo)
    print()
  except CloseException:
    print('Consulta detenida.\n')

@decor
def modificaciones(archivo_principal, archivo_consulta):
	try:
		ll = Looker_logger(archivo_principal, archivo_consulta)
		opcion = ll.closer('Que desea realizar:\n(1) añadir registro, (2) modificar registro,\n(3) borrar registro, (4) eliminar duplicados,\n(c) cerrar, (puede cerrar en cualquier momento ingresando "c"): ')
		print()
		if opcion == '1':
			tipo = ll.closer('Desea añadir ("equipo", "producto"): ')
			ll.add_rec()
			print()
		elif opcion == '2':
			id = int(ll.closer('Ingrese el id que desea modificar: '))
			columna = ll.closer('Ingrese exactamente la columna que desea modificar\n'
													'(producto, marca, modelo, calidad, color, stock, precio_usd, precio,\n'
													'descuento1, descuento2, precio_usd_modulo_bat, precio_modulo_bat): ')
			if columna == 'stock':
				print('En el caso de "stock" (deje el campo vacio y presione "enter" para que se tome como stock existente).')
			valor = ll.closer('Ingrese el nuevo valor con el que modificará el anterior: ')
			ll.change_prod_info(id=id, column=columna, value=valor)
			print()
		elif opcion == '3':
			id = int(ll.closer('Ingrese el id del registro que desea borrar: '))
			ll.delete_rec(id)
			print()
		elif opcion == '4':
			temp = ll.read_file()
			ll.no_duplicates(temp)
			print()
	except CloseException:
		print('Modificaciones detenidas.')

@decor
def menu_p(archivo_principal, archivo_consultas):
  while True:
    print('Menu principal.')
    res = input('(1) realizar modificaciones.\n(2) realizar consultas.\n(c) cerrar.\n')
    if res == '1':
      modificaciones(archivo_principal, archivo_consultas)
    if res == '2':
      consultas(archivo_principal, archivo_consultas)
    if res == 'c':
      print('Programa cerrado.')
      break

"""### execute"""

menu_p('_do_not_change_\_do_not_change_\_no_modificar_data_principal_productos_y_equipos_collection.csv',
       'archivos_consultas') ## modificar el nombre de la ruta a la carpeta de consultas
