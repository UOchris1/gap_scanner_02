import inspect
from src.core.database_operations import upsert_hit
print('\n'.join(inspect.getsource(upsert_hit).splitlines()))
