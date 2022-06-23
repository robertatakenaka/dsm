from scielo_classic_website.migration import (
    get_document_pids_to_migrate,
    get_paragraphs_id_file_path,
)
from scielo_classic_website import migration as classic_website_migration

from dsm.migration import db
from dsm import configuration


db.mk_connection(configuration.DATABASE_CONNECT_URL)


def create_mininum_record_in_isis_doc(pid, isis_updated_date):
    """
    Create a record in isis_doc only with pid, isis_updated_date and
    status == "pending_migration"
    """
    isis_document = (
        db.fetch_isis_document(pid) or
        db.create_isis_document()
    )
    if isis_document.isis_updated_date != isis_updated_date:
        isis_document._id = pid
        isis_document.isis_updated_date = isis_updated_date
        isis_document.update_status("PENDING_MIGRATION")
        db.save_data(isis_document)
        return {"pid": pid, "result": "done"}
    return {"pid": pid, "result": "skip"}


def register_isis_journal(_id, record):
    """
    Register migrated journal data

    Parameters
    ----------
    _id: str
    record : dict

    Returns
    -------
    str
        _id

    Raises
    ------
        dsm.storage.db.DBSaveDataError
        dsm.storage.db.DBCreateDocumentError
    """
    # recupera isis_journal ou cria se não existir

    isis_journal = (
        db.fetch_isis_journal(_id) or
        db.create_isis_journal()
    )
    isis_journal._id = _id
    isis_journal.record = record

    journal = classic_website_migration.Journal(record)
    isis_journal.isis_updated_date = journal.isis_updated_date
    isis_journal.isis_created_date = journal.isis_created_date

    # salva o journal
    db.save_data(isis_journal)
