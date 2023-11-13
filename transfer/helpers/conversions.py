from datetime import datetime


class Conversions:
    @staticmethod
    def get_conversion(source_type: str, target_type: str):
        if source_type.lower() == "string":
            if target_type.lower() == "date":
                return Conversions.string_to_date
        raise NotImplementedError(f"The conversion of {source_type} to {target_type} is not implemented!")

    @staticmethod
    def string_to_date(val, **kwargs):
        if val is None:
            return val
        if len(val) == 4:
            parsed = datetime.strptime(val, "%Y")
        elif len(val) == 7:
            parsed = datetime.strptime(val, "%Y-%M")
        else:
            parsed = datetime.strptime(val, kwargs["format"])
        return parsed.strftime("%Y-%m-%d")

    @staticmethod
    def do_nothing(val, **kwargs):
        return val
