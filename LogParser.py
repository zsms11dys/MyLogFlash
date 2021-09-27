import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.udf import udf
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from drain3.file_persistence import FilePersistence

persistence = FilePersistence("drain3_state.bin")
config = TemplateMinerConfig()
config.load(os.path.dirname(__file__) + "/drain3.ini")
config.profiling_enabled = False
template_miner = TemplateMiner(persistence, config)

@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def logParse(line : str) -> str:
    log_time, _, log_line = line.partition(' ')
    result = template_miner.add_log_message(log_line)
    if result['change_type'] != 'none':
        print('New template:', result['template_mined'])
    template_id = result['cluster_id']
    return f'{log_time} {template_id}'

source_ddl = """ 
CREATE TABLE input_log ( 
    log_line VARCHAR
) WITH ( 
    'connector' = 'filesystem', 
    'path' = 'file:///home/master/test.log'
) 
"""
sink_ddl = """ 
CREATE TABLE output_log ( 
    parsed_log_line VARCHAR
) WITH ( 
    'connector' = 'print' 
) 
"""

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env, environment_settings=EnvironmentSettings.new_instance().use_blink_planner().build())

t_env.sql_update(source_ddl)
t_env.sql_update(sink_ddl)
t_env.register_function('logParse', logParse)

t_env.sql_update('INSERT INTO output_log SELECT logParse(log_line) FROM input_log')
t_env.execute('LogParser')