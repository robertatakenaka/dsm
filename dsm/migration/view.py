"""
API for the migration
"""
from datetime import datetime

from dsm.extdeps.isis_migration import (
    migration_manager,
)

from dsm.migration import controller


_migration_manager = migration_manager.MigrationManager()
_migration_manager.db_connect()


_MIGRATION_PARAMETERS = {
    "title": dict(
        operations_sequence=[
            dict(
                name="REGISTER_ISIS",
                result="REGISTERED_ISIS_JOURNAL",
                action=_migration_manager.register_isis_journal,
            ),
            dict(
                name="PUBLISH",
                result="PUBLISHED_JOURNAL",
                action=_migration_manager.publish_journal_data,
            )
        ]
    ),
    "issue": dict(
        operations_sequence=[
            dict(
                name="REGISTER_ISIS",
                result="REGISTERED_ISIS_ISSUE",
                action=_migration_manager.register_isis_issue,
            ),
            dict(
                name="PUBLISH",
                result="PUBLISHED_ISSUE",
                action=_migration_manager.publish_issue_data,
            )
        ]
    ),
    "artigo": dict(
        operations_sequence=[
            dict(
                name="REGISTER_ISIS",
                result="REGISTERED_ISIS_DOCUMENT",
                action=_migration_manager.register_isis_document,
            ),
            dict(
                name="MIGRATE_DOCUMENT_FILES",
                result="MIGRATED_DOCUMENT_FILES",
                action=_migration_manager.migrate_document_files,
            ),
            dict(
                name="PUBLISH",
                result="PUBLISHED_DOCUMENT",
                action=_migration_manager.publish_document_metadata,
            ),
            dict(
                name="PUBLISH_PDFS",
                result="PUBLISHED_PDFS",
                action=_migration_manager.publish_document_pdfs,
            ),
            dict(
                name="PUBLISH_XMLS",
                result="PUBLISHED_XMLS",
                action=_migration_manager.publish_document_xmls,
            ),
            dict(
                name="PUBLISH_HTMLS",
                result="PUBLISHED_HTMLS",
                action=_migration_manager.publish_document_htmls,
            ),
        ]
    )
}


def list_documents_to_migrate(
        acron, issue_folder, pub_year, isis_updated_from, isis_updated_to,
        status=None,
        descending=None,
        page_number=None,
        items_per_page=None,
        ):
    return _migration_manager.list_documents_to_migrate(
        acron, issue_folder, pub_year, isis_updated_from, isis_updated_to,
        status=status,
        descending=descending,
        page_number=page_number,
        items_per_page=items_per_page,
    )


def migrate_document(pid):
    """
    Migrate ISIS records of a document

    Parameters
    ----------
    pid: str
        identifier in ISIS database

    Returns
    -------
    dict
        results of the migration
    """
    pids_and_their_records = controller.get_records_by_pid(pid)
    return _migrate_isis_records(pids_and_their_records, "artigo")


def migrate_isis_db(db_type, source_file_path):
    """
    Migrate ISIS database content from `source_file_path` or `records_content`
    which is ISIS database or ID file

    Parameters
    ----------
    db_type: str
        "title" or "issue" or "artigo"
    source_file_path: str
        ISIS database or ID file path


    Returns
    -------
    generator
    dict
         results of the migration
     """
    pids_and_their_records = controller.get_records_by_source_path(
        db_type, source_file_path)
    return _migrate_isis_records(pids_and_their_records, db_type)


def _migrate_isis_records(pids_and_their_records, db_type):
    """
    Migrate data from `source_file_path` which is ISIS database or ID file

    Parameters
    ----------
    id_file_records: generator or list of strings
        list of ID records
    db_type: str
        "title" or "issue" or "artigo"

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
    migration_parameters = _MIGRATION_PARAMETERS.get(db_type)
    if not migration_parameters:
        raise ValueError(
            "Invalid value for `db_type`. "
            "Expected values: title, issue, artigo"
        )

    for pid, records in pids_and_their_records:
        item_result = {"pid": pid}
        try:
            isis_data = records[0]
            operations_sequence = migration_parameters["operations_sequence"]
            if db_type == "artigo":
                # base artigo
                if len(records) == 1:
                    # registro de issue na base artigo
                    operations_sequence = (
                        _MIGRATION_PARAMETERS["issue"]["operations_sequence"]
                    )
                else:
                    # registros do artigo na base artigo
                    isis_data = records
            _result = _migrate_one_isis_item(
                pid, isis_data, operations_sequence,
            )
            item_result.update(_result)
        except Exception as e:
            item_result["error"] = str(e)
        yield item_result


def _migrate_one_isis_item(pid, isis_data, operations):
    """
    Migrate one ISIS item (title or issue or artigo)

    Parameters
    ----------
    pid: str
    isis_data: str

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


def migrate_acron(acron, id_folder_path=None):
    pids_and_their_records = controller.get_records_by_acron(acron, id_folder_path)
    return _migrate_isis_records(pids_and_their_records, "artigo")


def identify_documents_to_migrate(from_date=None, to_date=None):
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
    return controller.identify_documents_to_migrate(from_date, to_date)
