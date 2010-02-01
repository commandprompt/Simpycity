from exceptable.exceptable import Except, System
from simpycity import ProgrammingError, InternalError, NotFoundError #, PermissionError, UnknownUserError
# from simpycity.core import FunctionError # For backwards compat.



base = Except(InternalError, {
    'Exception': Exception,
    'NotFoundException': NotFoundError,
})

# system = System(ProgrammingError, {
#     'permission denied': PermissionError
# })

system = System(ProgrammingError, {})

