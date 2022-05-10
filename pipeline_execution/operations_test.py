from datetime import datetime
import pandas as pd
import pipelines.pipeline_execution.pipeline_structure as ps
import numpy as np
import pipelines.pipeline_execution.data as data

class LowerCase_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
        "value1": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat()
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "Lower Case", {"column_name": "name1"}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline("pipeline", input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["rohit", "tarun", "jishnu", "harshvardhan"],
            "value1": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_table()) == True

class UpperCase_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
        "value1": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat()
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "Upper Case", {"column_name": "name1"}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline("pipeline", input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["ROHIT", "TARUN", "JISHNU", "HARSHVARDHAN"],
            "value1": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_table()) == True

class AddValue_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
        "value1": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat()
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "Add Value", {"column_name": "value1", "value": 10}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline("pipeline", input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
            "value1": [20, 30, 40, 50],
            "key1": [1, 2, 3, 4],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_table()) == True

class MultiplyValue_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
        "value1": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat()
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "Multiply Value", {"column_name": "value1", "value": 10, "precision": 2}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline("pipeline", input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
            "value1": [100, 200, 300, 400],
            "key1": [1, 2, 3, 4],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_table()) == True

class DivideValue_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
        "value1": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat()
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "Divide Value", {"column_name": "value1", "value": 10, "precision": 2}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline("pipeline", input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
            "value1": [1.0, 2.0, 3.0, 4.0],
            "key1": [1, 2, 3, 4],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_table()) == True

class AddColumns_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
        "value1": [10, 20, 30, 40],
        "value2": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat()
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "Add Columns", {"column_name1": "value1", "column_name2": "value2", "inplace":True}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline("pipeline", input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
            "value1": [20, 40, 60, 80],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_table()) == True

class MultiplyColumns_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
        "value1": [10, 20, 30, 40],
        "value2": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat()
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "Multiply Columns", {"column_name1": "value1", "column_name2": "value2", "inplace":True, "precision": 2}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline("pipeline", input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
            "value1": [100, 400, 900, 1600],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_table()) == True

class DivideColumns_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
        "value1": [10, 20, 30, 40],
        "value2": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat()
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "Divide Columns", {"column_name1": "value1", "column_name2": "value2", "inplace":True, "precision": 2}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline("pipeline", input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
            "value1": [1.0, 1.0, 1.0, 1.0],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_table()) == True

class LengthOfText_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
        "value1": [10, 20, 30, 40],
        "value2": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat()
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "Length of Text", {"column_name": "name1", "output_column": "result"}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline("pipeline", input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
            "result": [5, 5, 6, 12],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_table()) == True

class SplitColumn_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
        "value1": [10, 20, 30, 40],
        "value2": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat()
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "Split Column by delimiter", {"column_name": "name1", "split_by": "_"}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline("pipeline", input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
            "name11": ["Rohit", "Tarun", "Jishnu", "Harshvardhan"],
            "name12": ["Katlaa", "Rayavaram", "Vinod", "Kumar"],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_table()) == True

class DateDiff_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
        "value1": [10, 20, 30, 40],
        "value2": [10, 20, 30, 40],
        "date1": [datetime.fromtimestamp(100), datetime.fromtimestamp(200), datetime.fromtimestamp(300), datetime.fromtimestamp(400)],
        "date2": [datetime.fromtimestamp(200), datetime.fromtimestamp(300), datetime.fromtimestamp(400), datetime.fromtimestamp(500)],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat()
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "Date Difference", {"column_name1": "date1", "column_name2": "date2", "output_column": "result"}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline("pipeline", input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "date1": [datetime.fromtimestamp(100), datetime.fromtimestamp(200), datetime.fromtimestamp(300), datetime.fromtimestamp(400)],
            "date2": [datetime.fromtimestamp(200), datetime.fromtimestamp(300), datetime.fromtimestamp(400), datetime.fromtimestamp(500)],
            "key1": [1, 2, 3, 4],
            "result": [np.timedelta64(100, 's'), np.timedelta64(100, 's'), np.timedelta64(100, 's'), np.timedelta64(100, 's')],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_table()) == True

class AddDate_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
        "value1": [10, 20, 30, 40],
        "value2": [10, 20, 30, 40],
        "date1": [datetime.fromtimestamp(100), datetime.fromtimestamp(200), datetime.fromtimestamp(300), datetime.fromtimestamp(400)],
        "date2": [datetime.fromtimestamp(100), datetime.fromtimestamp(200), datetime.fromtimestamp(300), datetime.fromtimestamp(400)],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat()
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "Add Date", {"column_name": "date1", "date": datetime.fromtimestamp(100)}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline("pipeline", input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "date1": [datetime.fromtimestamp(200), datetime.fromtimestamp(300), datetime.fromtimestamp(400), datetime.fromtimestamp(500)],
            "date2": [datetime.fromtimestamp(100), datetime.fromtimestamp(200), datetime.fromtimestamp(300), datetime.fromtimestamp(400)],
            "key1": [1, 2, 3, 4],
        })
        
        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_table()) == True

class IntFilter_test():

    def test():
        df1 = pd.DataFrame({
        "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
        "value1": [10, 20, 30, 40],
        "value2": [10, 20, 30, 40],
        "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat()
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "Integer Filter", {"column_name": "value1", "less_than_equal_to": 30}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline("pipeline", input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod"],
            "value1": [10, 20, 30],
            "value2": [10, 20, 30],
            "key1": [1, 2, 3],
        })

        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_table()) == True

class DeleteDuplicates_test():

    def test():
        df1 = pd.DataFrame({
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar", "Jishnu_Vinod"],
            "value1": [10, 20, 30, 40, 30],
            "value2": [10, 20, 30, 40, 30],
            "key1": [1, 2, 3, 4, 3],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat()
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "Delete Duplicate rows", {}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline("pipeline", input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["Rohit_Katlaa", "Tarun_Rayavaram", "Jishnu_Vinod", "Harshvardhan_Kumar"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })
        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_table()) == True

class RegExFilter_test():

    def test():
        df1 = pd.DataFrame({
            "name1": ["rohit_katlaa", "tarun_rayavaram", "jishnu_vinod", "harshvardhan_kumar", "JishnuVinod"],
            "value1": [10, 20, 30, 40, 30],
            "value2": [10, 20, 30, 40, 30],
            "key1": [1, 2, 3, 4, 3],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat()
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "RegEx filter", {"column_name": "name1", "regex": "[a-z]*_[a-z]"}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline("pipeline", input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["rohit_katlaa", "tarun_rayavaram", "jishnu_vinod", "harshvardhan_kumar"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })
        
        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_table()) == True

class AppendTables_test():

    def test():
        df1 = pd.DataFrame({
            "name1": ["rohit_katlaa", "tarun_rayavaram", "jishnu_vinod", "harshvardhan_kumar"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })

        df2 = pd.DataFrame({
            "name1": ["rohit_katlaa"],
            "value1": [10],
            "value2": [10],
            "key1": [1],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_table2 = data.DataTable("Data2", df2)
        input_data_dict = data.DataDict([input_table1, input_table2])

        p_format = ps.PipelineFormat()
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1","Data2"], "Append Tables", {"input_tables": ["Data1","Data2"], "output_name":"tarun"}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline("pipeline", input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["rohit_katlaa", "tarun_rayavaram", "jishnu_vinod", "harshvardhan_kumar", "rohit_katlaa"],
            "value1": [10, 20, 30, 40, 10],
            "value2": [10, 20, 30, 40, 10],
            "key1": [1, 2, 3, 4, 1],
        })
        
        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_table()) == True

class RenameColumn_test():

    def test():
        df1 = pd.DataFrame({
            "name1": ["rohit_katlaa", "tarun_rayavaram", "jishnu_vinod", "harshvardhan_kumar"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat()
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "Rename Column", {"column_name": "name1", "new_column_name": "Names"}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline("pipeline", input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
            "Names": ["rohit_katlaa", "tarun_rayavaram", "jishnu_vinod", "harshvardhan_kumar"],
        })
        
        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_table()) == True

class DeleteColumn_test():

    def test():
        df1 = pd.DataFrame({
            "name1": ["rohit_katlaa", "tarun_rayavaram", "jishnu_vinod", "harshvardhan_kumar"],
            "value1": [10, 20, 30, 40],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })

        input_table1 = data.DataTable("Data1", df1)
        input_data_dict = data.DataDict([input_table1])

        p_format = ps.PipelineFormat()
        s1_format = ps.StageFormat("Stage1")

        t_format1 = ps.TaskFormat(["Data1"], "Delete Column", {"column_name": "value1"}, "Data3")

        s1_format.add_task_format(t_format1)

        p_format.add_stage_format(s1_format)

        pipeline1 = ps.Pipeline("pipeline", input_data_dict, p_format)
        output = pipeline1.execute()

        df_expected_output = pd.DataFrame({
            "name1": ["rohit_katlaa", "tarun_rayavaram", "jishnu_vinod", "harshvardhan_kumar"],
            "value2": [10, 20, 30, 40],
            "key1": [1, 2, 3, 4],
        })
        
        assert df_expected_output.equals(output.fetch_dt("Data3").fetch_table()) == True

if __name__ == "__main__":
    # Erases the data.db file
    open('data.db', 'w').close()
    LowerCase_test.test()
    print("LowerCase_test executed successfully.")
    UpperCase_test.test()
    print("UpperCase_test executed successfully.")
    AddValue_test.test()
    print("AddValue_test executed successfully.")
    MultiplyValue_test.test()
    print("MultiplyValue_test executed successfully.")
    DivideValue_test.test()
    print("DivideValue_test executed successfully.")
    AddColumns_test.test()
    print("AddColumns_test executed successfully.")
    MultiplyColumns_test.test()
    print("MultiplyColumns_test executed successfully.")
    DivideColumns_test.test()
    print("DivideColumns_test executed successfully.")
    LengthOfText_test.test()
    print("LengthOfText_test executed successfully.")
    SplitColumn_test.test()
    print("SplitColumn_test executed successfully.")
    DateDiff_test.test()
    print("DateDiff_test executed successfully.")
    AddDate_test.test()
    print("AddDate_test executed successfully.")
    IntFilter_test.test()
    print("IntFilter_test executed successfully.")
    DeleteDuplicates_test.test()
    print("DeleteDuplicates_test executed successfully.")
    RegExFilter_test.test()
    print("RegExFilter_test executed successfully.")
    AppendTables_test.test()
    print("AppendTables_test executed successfully.")
    RenameColumn_test.test()
    print("RenameColumn_test executed successfully.")
    DeleteColumn_test.test()
    print("DeleteColumn_test executed successfully.")
