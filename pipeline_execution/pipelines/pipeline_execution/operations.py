from abc import abstractmethod
from datetime import datetime
import pipelines.pipeline_execution.data as data
import pandas as pd


def schema_params(required_params: dict, operation_params: dict) -> bool:
  '''
  required_params = {params : type}
  operation_params = {params : value}
  '''
  if len(required_params) != len(operation_params):
    return False
  for k, v in operation_params.items():
    if required_params.get(k) == None:
      return False
    elif type(v) != required_params[k]:
      return False
  return True


class BaseOperation:


  @staticmethod
  @abstractmethod
  def validate_params(operation_params: dict) -> bool:
    pass

  @staticmethod
  def get_type() -> str:
    return "BaseOperation"


class SimpleOperation(BaseOperation):

  @staticmethod
  @abstractmethod
  def run(operation_params: dict, inp_dt: data.DataTable) -> data.DataTable:
    pass

  @staticmethod
  def get_type() -> str:
    return "SimpleOperation"


class ComplexOperation(BaseOperation):

  @staticmethod
  @abstractmethod
  def run(operation_params: dict, inp_dd: data.DataDict) -> data.DataTable:
    pass

  @staticmethod
  def get_type() -> str:
    return "ComplexOperation"

class RenameColumnOperation(SimpleOperation):
  
  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str, "new_column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dt: data.DataTable):
    df = inp_dt.fetch_table()
    df[operation_params["new_column_name"]] = df[operation_params["column_name"]]
    df.pop(operation_params["column_name"])
    dt = data.DataTable(inp_dt.fetch_name(), df)

    return dt

class DeleteColumnOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dt: data.DataTable):
    df = inp_dt.fetch_table()
    df.pop(operation_params["column_name"])
    dt = data.DataTable(inp_dt.fetch_name(), df)
    return dt

class IntFilterOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:
    optional_params = ["less_than_equal_to", "greater_than_equal_to", "less_than", "greater_than", "equal_to"]
    required_params = ["column_name"]
    if operation_params["column_name"] != None:
      has_optional_param = False
      for optional_param in optional_params:
        if operation_params.get(optional_param) != None: 
          if type(operation_params[optional_param]) in [int, float]:
            has_optional_param = True
      if has_optional_param:
        return True
    return Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dt: data.DataTable):
    df = inp_dt.fetch_table()

    if df[operation_params["column_name"]].dtype == 'int64' or df[operation_params["column_name"]].dtype == 'float64':
      if operation_params.get('less_than_equal_to') != None:
        df = df[df[operation_params['column_name']] <= operation_params['less_than_equal_to']]

      if operation_params.get('greater_than_equal_to') != None:
        df = df[df[operation_params['column_name']] >= operation_params['greater_than_equal_to']]

      if operation_params.get('less_than') != None:
        df = df[df[operation_params['column_name']] < operation_params['less_than']]

      if operation_params.get('greater_than') != None:
        df = df[df[operation_params['column_name']] > operation_params['greater_than']]

      if operation_params.get('equal_to') != None:
        df = df[df[operation_params['column_name']] == operation_params['equal_to']]

    else:
      raise Exception("Either column type should be integer or a decimal")
    
    dt = data.DataTable(inp_dt.fetch_name(), df)
    return dt

class DeleteDuplicatesOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:
      return True

  def run(operation_params: dict, inp_dt: data.DataTable):
    df = inp_dt.fetch_table()

    df = df.drop_duplicates(keep='first',inplace=False)
    dt = data.DataTable(inp_dt.fetch_name(), df)
    return dt

class RegExFilter(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"column_name": str, "regex": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable):
    df = inp_dt.fetch_table()
    df = df[df[operation_params["column_name"]].str.match(operation_params["regex"])]
    dt = data.DataTable(inp_dt.fetch_name(), df)
    return dt

class LowerCaseOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable):
    dt = data.DataTable(inp_dt.fetch_name(), inp_dt.fetch_table())

    dt.fetch_table()[operation_params["column_name"]] = dt.fetch_table()[operation_params["column_name"]].apply(lambda x: x.lower())

    return dt
  
class UpperCaseOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable):
    dt = data.DataTable(inp_dt.fetch_name(), inp_dt.fetch_table())

    dt.fetch_table()[operation_params["column_name"]] = dt.fetch_table()[operation_params["column_name"]].apply(lambda x: x.upper())

    return dt

class AddValueOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str, "value": int}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable):
    dt = data.DataTable(inp_dt.fetch_name(), inp_dt.fetch_table())

    dt.fetch_table()[operation_params["column_name"]] = dt.fetch_table()[operation_params["column_name"]].apply(lambda x: x + operation_params["value"])

    return dt

class MultiplyValueOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str, "value": int, "precision": int}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable):
    dt = data.DataTable(inp_dt.fetch_name(), inp_dt.fetch_table())

    dt.fetch_table()[operation_params["column_name"]] = dt.fetch_table()[operation_params["column_name"]].apply(lambda x: x * operation_params["value"])

    return dt

class DivideValueOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str, "value": int, "precision": int}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable):
    dt = data.DataTable(inp_dt.fetch_name(), inp_dt.fetch_table())

    dt.fetch_table()[operation_params["column_name"]] = dt.fetch_table()[operation_params["column_name"]].apply(lambda x: x / operation_params["value"])

    return dt

class AddColumnsOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    if operation_params.get("inplace") != None:
      if operation_params["inplace"] == False:
        required_params = {"column_name1":str, "column_name2":str, "inplace":bool, "output_column":str}
        if schema_params(required_params, operation_params):
          return True
        else:
          raise Exception("Either parameter column_name is not present or given more parameters")
      else:
        required_params = {"column_name1":str, "column_name2":str, "inplace":bool}
        if schema_params(required_params, operation_params):
          return True
        else:
          raise Exception("Either parameter column_name is not present or given more parameters")
    else:
      raise Exception("Either parameter column_name is not present or given more parameters")

  def run(operation_params: dict, inp_dt: data.DataTable):
    dt = data.DataTable(inp_dt.fetch_name(), inp_dt.fetch_table())

    if(operation_params.get("inplace") == True):
      dt.fetch_table()[operation_params["column_name1"]] = dt.fetch_table()[operation_params["column_name1"]] + dt.fetch_table()[operation_params["column_name2"]]
    else:
      dt.fetch_table()[operation_params["output_column"]] = dt.fetch_table()[operation_params["column_name1"]] + dt.fetch_table()[operation_params["column_name2"]]

    return dt

class MultiplyColumnsOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    if operation_params.get("inplace") != None:
      if operation_params["inplace"] == False:
        required_params = {"column_name1":str, "column_name2":str, "inplace":bool, "output_column":str, "precision": int}
        if schema_params(required_params, operation_params):
          return True
        else:
          raise Exception("Either parameter column_name is not present or given more parameters")
      else:
        required_params = {"column_name1":str, "column_name2":str, "inplace":bool, "precision": int}
        if schema_params(required_params, operation_params):
          return True
        else:
          raise Exception("Either parameter column_name is not present or given more parameters")
    else:
      raise Exception("Either parameter column_name is not present or given more parameters")


  def run(operation_params: dict, inp_dt: data.DataTable):
    dt = data.DataTable(inp_dt.fetch_name(), inp_dt.fetch_table())

    if(operation_params.get("inplace") == True):
      dt.fetch_table()[operation_params["column_name1"]] = dt.fetch_table()[operation_params["column_name1"]] * dt.fetch_table()[operation_params["column_name2"]]
    else:
      dt.fetch_table()[operation_params["output_column"]] = dt.fetch_table()[operation_params["column_name1"]] * dt.fetch_table()[operation_params["column_name2"]]

    return dt

class DivideColumnsOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    if operation_params.get("inplace") != None:
      if operation_params["inplace"] == False:
        required_params = {"column_name1":str, "column_name2":str, "inplace":bool, "output_column":str, "precision": int}
        if schema_params(required_params, operation_params):
          return True
        else:
          raise Exception("Either parameter column_name is not present or given more parameters")
      else:
        required_params = {"column_name1":str, "column_name2":str, "inplace":bool, "precision": int}
        if schema_params(required_params, operation_params):
          return True
        else:
          raise Exception("Either parameter column_name is not present or given more parameters")
    else:
      raise Exception("Either parameter column_name is not present or given more parameters")


  def run(operation_params: dict, inp_dt: data.DataTable):
    dt = data.DataTable(inp_dt.fetch_name(), inp_dt.fetch_table())

    if(operation_params.get("inplace") == True):
      dt.fetch_table()[operation_params["column_name1"]] = dt.fetch_table()[operation_params["column_name1"]] / dt.fetch_table()[operation_params["column_name2"]]
    else:
      dt.fetch_table()[operation_params["output_column"]] = dt.fetch_table()[operation_params["column_name1"]] / dt.fetch_table()[operation_params["column_name2"]]

    return dt

class LengthOfTextOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str, "output_column": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable):
    dt = data.DataTable(inp_dt.fetch_name(), inp_dt.fetch_table())

    if(dt.fetch_table()[operation_params["column_name"]].dtype == 'object'):
      dt.fetch_table()[operation_params["output_column"]] = dt.fetch_table()[operation_params["column_name"]].apply(lambda x: len(x))
    else:
      raise Exception("Cannot apply length operation on non-string values")

    return dt

class DateDiffOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name1": str, "column_name2": str, "output_column": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either parameter column_name is not present or given more parameters")


  def run(operation_params: dict, inp_dt: data.DataTable):
    dt = data.DataTable(inp_dt.fetch_name(), inp_dt.fetch_table())

    d1 = dt.fetch_table()[operation_params["column_name1"]].apply(lambda x: pd.Timestamp(x))
    d2 = dt.fetch_table()[operation_params["column_name2"]].apply(lambda x: pd.Timestamp(x))
    dt.fetch_table()[operation_params["output_column"]] = d2-d1

    return dt

class AddDateOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str, "date": datetime}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable):
    dt = data.DataTable(inp_dt.fetch_name(), inp_dt.fetch_table())

    d = dt.fetch_table()[operation_params["column_name"]].apply(lambda x: pd.Timestamp(x))
    dt.fetch_table()[operation_params["column_name"]] = d.apply(lambda x: x - pd.Timestamp(datetime.fromtimestamp(0)) + pd.Timestamp(operation_params["date"]))

    return dt

class SplitColumnOperation(SimpleOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"column_name": str, "split_by": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dt: data.DataTable):
    dt = data.DataTable(inp_dt.fetch_name(), inp_dt.fetch_table())

    st = dt.fetch_table()[operation_params["column_name"]].iloc[0]
    st_list = st.split(operation_params["split_by"])
    st_len = len(st_list)

    list = [operation_params["column_name"]+str(i+1) for i in range(st_len)]

    dt.fetch_table()[list] = dt.fetch_table()[operation_params["column_name"]].apply(lambda x: pd.Series(str(x).split("_")))

    return dt


class JoinTableOperation(ComplexOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"table1":str, "table2":str, "left_on":str, "right_on":str, "type":str}
    if schema_params(required_params, operation_params):
      return True
    return Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dd: data.DataDict):
    dt1 = inp_dd.fetch_dt(operation_params["table1"])
    dt2 = inp_dd.fetch_dt(operation_params["table2"])
    dt = data.DataTable(
      dt1.fetch_name(),
      dt1.fetch_table().merge(
        dt2.fetch_table(), left_on=operation_params["left_on"], right_on=operation_params["right_on"], how=operation_params["type"]))
    return dt
    
class AddVariableOperation(ComplexOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"table_name": str, "column_name": str, "variable": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dd: data.DataDict):
    df = inp_dd.fetch_dt(operation_params["table_name"]).fetch_table()

    value = inp_dd.fetch_dt(operation_params["variable"]).fetch_table()
    df[operation_params["column_name"]] = df[operation_params["column_name"]].apply(lambda x: x + value)

    dt = data.DataTable(operation_params["table_name"], df)
    return dt


class SubVariableOperation(ComplexOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"table_name": str, "column_name": str, "variable": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dd: data.DataDict):
    df = inp_dd.fetch_dt(operation_params["table_name"]).fetch_table()

    value = inp_dd.fetch_dt(operation_params["variable"]).fetch_table()
    df[operation_params["column_name"]] = df[operation_params["column_name"]].apply(lambda x: x - value)

    dt = data.DataTable(operation_params["table_name"], df)
    return dt

class MultiplyVariableOperation(ComplexOperation):

  def validate_params(operation_params: dict) -> bool:

    required_params = {"table_name": str, "column_name": str, "variable": str}

    if schema_params(required_params, operation_params):
      return True
    else:
      raise Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")


  def run(operation_params: dict, inp_dd: data.DataDict):
    df = inp_dd.fetch_dt(operation_params["table_name"]).fetch_table()

    value = inp_dd.fetch_dt(operation_params["variable"]).fetch_table()
    df[operation_params["column_name"]] = df[operation_params["column_name"]].apply(lambda x: x*value)

    dt = data.DataTable(operation_params["table_name"], df)
    return dt

class AppendTablesOperation(ComplexOperation):

  def validate_params(operation_params: dict) -> bool:
    required_params = {"input_tables":list, "output_name":str}
    if schema_params(required_params, operation_params):
      return True
    return Exception("Either one of the parameters are missing or the mentioned parameter(s) not required by this operation")

  def run(operation_params: dict, inp_dd: data.DataDict):

    df = inp_dd.fetch_dt(operation_params["input_tables"][0]).fetch_table()

    for i in range(1, len(operation_params["input_tables"])):
      df2 = inp_dd.fetch_dt(operation_params["input_tables"][i]).fetch_table()
      df = df.append(df2, ignore_index = True)

    dt = data.DataTable(operation_params["output_name"], df)
    return dt