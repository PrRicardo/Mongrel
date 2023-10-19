from datetime import datetime


class Conversions:
    @staticmethod
    def get_conversion(source_type: str, target_type: str):
        if source_type.lower == "string":
            if target_type.lower() == "date":
                return Conversions.string_to_date
        raise NotImplementedError(f"The conversion of {source_type} to {target_type} is not implemented!")

    @staticmethod
    def string_to_date(val, formatting="%Y-%m-%d"):
        parsed = datetime.strptime(val, formatting)
        return parsed.strftime("%Y-%m-%d")
