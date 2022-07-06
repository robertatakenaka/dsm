from datetime import datetime

from mongoengine import connect, Q

from dsm import exceptions, configuration


mk_connection(configuration.DATABASE_CONNECT_URL)


def convert_date(date):
    """Traduz datas em formato YYYYMMDD ou ano-mes-dia, ano-mes para
    o formato usado no banco de dados"""

    _date = None
    if "-" not in date:
        date = "-".join((date[:4], date[4:6], date[6:]))

    def limit_month_range(date):
        """Remove o segundo mês da data limitando o intervalo de criaçã
        >>> limit_month_range("2018-Oct-Dec")
        >>> "2018-Dec"
        """
        parts = [part for part in date.split("-") if len(part.strip()) > 0]
        return "-".join([parts[0], parts[-1]])

    def remove_invalid_date_parts(date):
        """Remove partes inválidas de datas e retorna uma data válida
        >>> remove_invalid_date_parts("2019-12-100")
        >>> "2019-12"
        >>> remove_invalid_date_parts("2019-20-01")
        >>> "2019" # Não faz sentido utilizar o dia válido após um mês inválido
        """
        date = date.split("-")
        _date = []

        for index, part in enumerate(date):
            if len(part) == 0 or part == "00" or part == "0":
                break
            elif index == 1 and part.isnumeric() and int(part) > 12:
                break
            elif index == 2 and part.isnumeric() and int(part) > 31:
                break
            elif part.isdigit():
                part = str(int(part))
            _date.append(part)

        return "-".join(_date)

    formats = [
        ("%Y-%m-%d", lambda x: x),
        ("%Y-%m", lambda x: x),
        ("%Y", lambda x: x),
        ("%Y-%b-%d", lambda x: x),
        ("%Y-%b", lambda x: x),
        ("%Y-%B", lambda x: x),
        ("%Y-%B-%d", lambda x: x),
        ("%Y-%B", remove_invalid_date_parts),
        ("%Y-%b", limit_month_range),
        ("%Y-%m-%d", remove_invalid_date_parts),
        ("%Y-%m", remove_invalid_date_parts),
        ("%Y", remove_invalid_date_parts),
    ]

    for template, func in formats:
        try:
            _date = (
                datetime.strptime(func(date.strip()), template).isoformat(
                    timespec="microseconds"
                )
                + "Z"
            )
        except ValueError:
            continue
        else:
            return _date

    raise ValueError("Could not transform date '%s' to ISO format" % date) from None    


def mk_connection(host):
    try:
        connect(host=host)
    except Exception as e:
        raise exceptions.DBConnectError(e)


def fetch_record(_id, model, **kwargs):
    try:
        obj = model.objects(_id=_id, **kwargs)[0]
    except IndexError:
        return None
    except Exception as e:
        raise exceptions.DBFetchMigratedDocError(e)
    else:
        return obj


def fetch_records(model, **kwargs):
    try:
        objs = model.objects(**kwargs)
    except IndexError:
        return None
    except Exception as e:
        raise exceptions.DBFetchMigratedDocError(e)
    else:
        return objs


def save_data(data):
    if not hasattr(data, 'created'):
        data.created = None
    try:
        data.updated = datetime.utcnow()
        if not data.created:
            data.created = data.updated
        data.save()
        return data
    except Exception as e:
        raise
        # exceptions.DBSaveDataError(e)
