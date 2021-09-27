import numpy as np
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from multiprocessing import shared_memory

source_ddl = ''
sink_ddl = ''

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env, environment_settings=EnvironmentSettings.new_instance().use_blink_planner().build())

t_env.sql_update(source_ddl)
t_env.sql_update(sink_ddl)

share_A = shared_memory.SharedMemory(name='matrix_A')
share_T = shared_memory.SharedMemory(name='matrix_T')

A = np.ndarray(buffer=share_A.buf)
T = np.ndarray(buffer=share_T.buf)