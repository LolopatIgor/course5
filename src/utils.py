def convert_salary(currency_value, currency_name):
    # Конвертор к рублям из валют
    if currency_value is None or currency_value is None:
        return None
    if currency_name.upper() == "USD":
        return currency_value * 88.45
    elif currency_name.upper() == "UZS":
        return currency_value * 0.007
    elif currency_name.upper() == "BYR":
        return currency_value * 27.03
    elif currency_name.upper() == "EUR":
        return currency_value * 95.17
    elif currency_name.upper() == "KZT":
        return currency_value * 0.19
    else:
        return currency_value
