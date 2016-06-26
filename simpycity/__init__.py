"""
    COPYRIGHT 2008-2016 Command Prompt, Inc.
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lessor General Public License as published by
    the Free Software Foundation.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import psycopg2

DataError = psycopg2.DataError
DatabaseError = psycopg2.DatabaseError
IntegrityError = psycopg2.IntegrityError
InternalError = psycopg2.InternalError
InterfaceError = psycopg2.InterfaceError
OperationalError = psycopg2.OperationalError
ProgrammingError = psycopg2.ProgrammingError
NotSupportedError = psycopg2.NotSupportedError


class PermissionError(InternalError):
    pass

class UnknownUserError(PermissionError):
    pass

class NotFoundError(InternalError):
    pass

class CannotSave(InternalError):
    pass

