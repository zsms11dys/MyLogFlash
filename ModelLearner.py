import pickle
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

_A = pickle.load('TCFG_A')
_T = pickle.load('TCFG_T')

share_A = shared_memory.SharedMemory(name='matrix_A', create=True, size=_A.nbytes)
A = np.ndarray(_A.shape, dtype=_A.dtype, buffer=share_A.buf)
A[:,:] = _A[:,:]

share_T = shared_memory.SharedMemory(name='matrix_T', create=True, size=_T.nbytes)
T = np.ndarray(_T.shape, dtype=_T.dtype, buffer=share_T.buf)
T[:,:] = _T[:,:]