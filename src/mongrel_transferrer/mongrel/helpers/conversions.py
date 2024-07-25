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

This file contains the conversions used for all the datatypes. Add new conversion functions here.
example:
    ...
    "transfer_options": {
        "conversion_fields": {
            "release_date": {
                "source_type": "string",
                "target_type": "date",
                "args": {
                    "format": "%Y-%m-%d"
                }
            }
        }
    }
This configuration will be parsed the following:
1. The get_conversion method gets called with source_type=string and target_type=date
2. The returned string_to_date function will be called everytime a release_date field is read with
val=release_date_value and kwargs={"format": "%Y-%m-%d"}
3. The converted value is then stored to be written to the database
"""
from datetime import datetime


class Conversions:
    """
    This class stores all the conversion functions and needs to be extended for other use cases
    """

    @staticmethod
    def get_conversion(source_type: str, target_type: str):
        """
        This method is used to pick a conversion function for the corresponding data types
        :param source_type: the source type to convert of off
        :param target_type: the target data type to convert to
        :return: return the correct conversion function
        :raises: NotImplementedError if no correct data conversion can be found
        """
        if source_type.lower() == "string":
            if target_type.lower() == "date":
                return Conversions.string_to_date
            if target_type.lower() == "string":
                return Conversions.remove_null_characters
            if target_type.lower() == "spotify_date":
                return Conversions.string_to_spotify_date
        if source_type.lower() == "object":
            if target_type.lower() == "string":
                return Conversions.object_to_str
        raise NotImplementedError(f"The conversion of {source_type} to {target_type} is not implemented!")

    @staticmethod
    def object_to_str(val: object):
        """
        calls the to string method of val
        :param val: the value that needs to be converted
        :return: string of value
        """
        return str(val)

    @staticmethod
    def string_to_spotify_date(val: str, **kwargs):
        """
        Takes a string and parses it to a date. This is currently pretty hard-coded for the spotify use case and needs
        to be generified
        :param val: the value that needs to be converted
        :param kwargs: these keyword arguments get filled with the args given in the mapping file
        :return: the converted value
        """
        if val is None or val == "0000":
            return None
        if len(val) == 4:
            parsed = datetime.strptime(val, "%Y")
        elif len(val) == 7:
            parsed = datetime.strptime(val, "%Y-%M")
        else:
            parsed = datetime.strptime(val, kwargs["format"])
        return parsed.strftime("%Y-%m-%d")

    @staticmethod
    def string_to_date(val: str, **kwargs):
        """
        Takes a string and parses it to a date.
        :param val: the value that needs to be converted
        :param kwargs: these keyword arguments get filled with the args given in the mapping file
        :return: the converted value
        """
        if val is None or val == "":
            return None
        formats = kwargs["input_format"]
        if not isinstance(formats, list):
            formats = [formats]
        for form in formats:
            try:
                parsed = datetime.strptime(val, form)
                return parsed.strftime(kwargs["output_format"])
            except ValueError:
                pass
        raise ValueError(f"Value {val} could not be converted with the given formats {formats}")

    @staticmethod
    def do_nothing(val: object):
        """
        this is the default value of the conversion functionality, it simplifies the function calling during the
        transfer
        :param val: the value
        :return: just the initial value 🤓
        """
        return val

    @staticmethod
    def remove_null_characters(val: str):
        """
        Removes null characters from a string if there are issues writing to postgres
        :param val: the value that needs to be converted
        :return: the converted value
        """
        return val.replace("\x00", "")
