import os
import builtins
from typing import Callable

# Default open context manager
BUILTIN_OPEN: Callable = builtins.open
BUILTIN_LISTDIR: Callable = os.listdir
BUILTIN_PATH_JOIN: Callable = os.path.join
BUILTIN_ISDIR: Callable = os.path.isdir

class NDTiffFileIO:

    def __init__(self,
                 open_function: Callable = BUILTIN_OPEN,
                 listdir_function: Callable = BUILTIN_LISTDIR,
                 path_join_function: Callable = BUILTIN_PATH_JOIN, 
                 isdir_function: Callable = BUILTIN_ISDIR):
        """Define a group of IO functions for use within this module."""
        self.open = open_function
        self.listdir = listdir_function
        self.path_join = path_join_function
        self.isdir = isdir_function

# Default values
BUILTIN_FILE_IO = NDTiffFileIO(open_function = BUILTIN_OPEN,
                               listdir_function = BUILTIN_LISTDIR,
                               path_join_function = BUILTIN_PATH_JOIN, 
                               isdir_function = BUILTIN_ISDIR)