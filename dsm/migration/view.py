"""
API for the migration
"""

from dsm.extdeps.isis_migration import (
    migration_manager,
)

from dsm.migration import controller, publication


_migration_manager = migration_manager.MigrationManager()
_migration_manager.db_connect()


_MIGRATION_PARAMETERS = {
    "title": dict(
        operations_sequence=[
            dict(
                name="REGISTER_ISIS",
                result="REGISTERED_ISIS_JOURNAL",
                action=controller.register_isis_journal,
            ),
            dict(
                name="PUBLISH",
                result="PUBLISHED_JOURNAL",
                action=publication.publish_journal_data,
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
    Migrate ISIS database content from `source_file_path`
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
    return controller.migrate_isis_records(
        pids_and_their_records,
        _MIGRATION_PARAMETERS.get(db_type),
        db_type == "artigo" and _MIGRATION_PARAMETERS["issue"],
    )


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
