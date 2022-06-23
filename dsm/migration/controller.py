from datetime import datetime

from scielo_classic_website.migration import (
    get_document_pids_to_migrate,
    get_records_by_pid,
    get_records_by_source_path,
    get_records_by_acron,
)

from dsm.migration import tasks, _controller, publication


def identify_documents_to_migrate(from_date, to_date):
    """
    Obtém do índice da bases de dados ISIS os PIDs registrados
    Registra na base de dados MongoDB que controla o conteúdo migrado

    Parameters
    ---------
    from_date (YYYYMMDD): str
    to_date (YYYYMMDD): str

    Returns
    -------
    None
    """
    for doc in get_document_pids_to_migrate(from_date, to_date):
        tasks.create_mininum_record_in_isis_doc(
            doc["pid"], doc["updated"]
        )


def migrate_isis_records(pids_and_their_records, migration_parameters, issue_migration_operations=None):
    """
    Migrate data from `source_file_path` which is ISIS database or ID file

    Parameters
    ----------
    pids_and_their_records: sequence of (id, records)
    migration_parameters: list
        "title" or "issue" or "artigo"
    issue_migration_operations: list of dict

    Returns
    -------
    generator

    ```
        {
            "pid": "pid",
            "events": [
                {
                    "_id": "",
                    "event": "",
                    "isis_created": "",
                    "isis_updated": "",
                    "created": "",
                    "updated": "",
                },
                {
                    "_id": "",
                    "event": "",
                    "created": "",
                    "updated": "",
                }
            ]
        }
        ``` 
        or 
    ```
        {
            "pid": "pid",
            "error": ""
        }
    ```

    Raises
    ------
        ValueError

    """
    # get the migration parameters according to db_type:
    # title or issue or artigo
    if not migration_parameters:
        raise ValueError(
            "Invalid value for `db_type`. "
            "Expected values: title, issue, artigo"
        )

    for pid, records in pids_and_their_records:
        yield _migrate_pid_records(
            pid, records,
            migration_parameters,
            issue_migration_operations,
        )


def _migrate_pid_records(pid, records, migration_parameters, issue_migration_operations):
    item_result = {"pid": pid}
    try:
        isis_data, operations_sequence = _get_parameters_for__migrate_one_isis_item(
            records, migration_parameters, issue_migration_operations,
        )
        _result = _migrate_one_isis_item(
            pid, isis_data, operations_sequence,
        )
        item_result.update(_result)
    except Exception as e:
        item_result["error"] = str(e)
    return item_result


def _get_parameters_for__migrate_one_isis_item(records, migration_parameters, issue_migration_operations):
    isis_data = records[0]
    operations_sequence = migration_parameters["operations_sequence"]
    if issue_migration_operations:
        # base artigo
        if len(records) == 1:
            # registro de issue na base artigo
            operations_sequence = issue_migration_operations
        else:
            # registros do artigo na base artigo
            isis_data = records
    return isis_data, operations_sequence


def _migrate_one_isis_item(pid, isis_data, operations):
    """
    Migrate one ISIS item (title or issue or artigo)

    Parameters
    ----------
    pid: str
    isis_data: dict or list of dict

    Returns
    -------
    dict
        {
            "pid": "",
            "events": [],
        }
    """
    result = {
        "pid": pid,
    }
    events = []
    try:
        op = operations[0]
        saved = op['action'](pid, isis_data)
        events.append(
            _get_event(op, saved,
                       saved[0].isis_created_date, saved[0].isis_updated_date)
        )
    except Exception as e:
        events.append(_get_error(op, e))

    for op in operations[1:]:
        try:
            saved = op['action'](pid)
            events.append(_get_event(op, saved))
        except Exception as e:
            events.append(_get_error(op, e))
    result["events"] = events
    return result


def _get_error(operation, error):
    return {
        "op": operation,
        "error": str(error),
        "timestamp": datetime.utcnow().isoformat(),
    }


def _get_event(operation, saved, isis_created_date=None, isis_updated_date=None):
    if not saved:
        return {
            "event_name": operation["name"],
            "event_result": operation["result"],
        }

    record_data, tracker = saved
    event = {
        "_id": record_data._id,
        "event_name": operation["name"],
        "event_result": operation["result"],
        "created": record_data.created,
        "updated": record_data.updated,
    }
    if tracker:
        event.update({
            "detail": tracker.detail,
            "total errors": tracker.total_errors,
        })
        event.update(tracker.status)
    if isis_created_date and isis_updated_date:
        event.update({
            "isis_created": isis_created_date,
            "isis_updated": isis_updated_date,
        })
    return event


def register_isis_journal(_id, record):
    return _controller.register_isis_journal(_id, record)

