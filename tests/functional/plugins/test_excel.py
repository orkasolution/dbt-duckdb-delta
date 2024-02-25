import pandas
import pytest

from dbt.tests.util import (
    check_relations_equal,
    run_dbt,
)

schema_yml = """
version: 2
sources:
  - name: excel_source
    schema: main
    meta:
      plugin: excel
    tables:
      - name: excel_file
        description: "An excel file"
        meta:
          external_location: "{test_data_path}/excel_file.xlsx"
"""

#Question why is file here defined? this is not a source this is config?
plugins = [
    {
        "module": "excel",
        "config": {
            "output": {
                "engine": "openpyxl"
            }
        }
    },
]

model_sql = """
    {{ config(materialized='external', plugin='excel') }}
    select * from {{ source('excel_source', 'excel_file') }}
"""


class TestExcelPlugin:
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        return {
            "test": {
                "outputs": {
                    "dev": {
                        "type": "duckdb",
                        "path": dbt_profile_target.get("path", ":memory:"),
                        "plugins": plugins,
                    }
                },
                "target": "dev",
            }
        }

    @pytest.fixture(scope="class")
    def models(self, test_data_path):
        return {
            "schema_excel.yml": schema_yml.format(test_data_path=test_data_path),
            "excel_read_write.sql": model_sql,
        }

    def test_excel_plugin(self, project):
        results = run_dbt()
        assert len(results) == 1

        res = project.run_sql("SELECT COUNT(1) FROM excel_file", fetch="one")
        assert res[0] == 9

        df = pandas.read_excel('./excel_read_write.xlsx')
        assert df.shape[0] == 9
        assert df['First Name'].iloc[0] == 'Dulce'

        check_relations_equal(
            project.adapter,
            [
                "excel_file",
                "excel_read_write",
            ],
        )


#TODO write more tests
        
#TODO here can be the problem that i deleted some file output from the config 
#which doesnt makes so much sense but is a breaking change 