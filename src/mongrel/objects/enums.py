"""
    MONGREL: MONgodb Going RELational
    Copyright (C) 2023 Ricardo Prida

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.

This file defines enums
"""

from enum import Enum


class ConflictHandling(Enum):
    """
    This enum describes the different choices of error handling.
    """
    NONE = 1
    """
    Choose value None to add ON CONFLICT DO NOTHING at the end of its inserts.
    """
    TRUNCATE = 2
    """
    TRUNCATE truncates previous tables with the same schema and table name as the target tables
    """
    DROP = 3
    """
    DROP previous tables with the same schema and table name as the target tables
    """
