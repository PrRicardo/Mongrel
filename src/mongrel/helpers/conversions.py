"""
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
        raise NotImplementedError(f"The conversion of {source_type} to {target_type} is not implemented!")

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
        parsed = datetime.strptime(val, kwargs["input_format"])
        return parsed.strftime(kwargs["output_format"])

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
        val.replace("\x00", "")
        return val
