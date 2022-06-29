import os

from scielo_classic_website.migration import (
    get_document_pids_to_migrate,
    get_paragraphs_records,
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


def register_isis_issue(_id, record):
    """
    Register migrated issue data

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
    # recupera isis_issue ou cria se não existir
    registered = (
        db.fetch_isis_issue(_id) or
        db.create_isis_issue()
    )

    issue = classic_website_migration.Issue(record)

    registered._id = _id
    registered.isis_updated_date = issue.isis_updated_date
    registered.isis_created_date = issue.isis_created_date
    registered.record = issue.record

    # salva o issue
    db.save_data(registered)


def register_isis_document(_id, records):
    """
    Register migrated document data

    Parameters
    ----------
    _id: str
    records : list of dict

    Returns
    -------
    str
        _id

    Raises
    ------
        dsm.storage.db.DBSaveDataError
        dsm.storage.db.DBCreateDocumentError
    """
    # recupera `isis_document` ou cria se não existir

    # se existirem osregistros de parágrafos que estejam externos à
    # base artigo, ou seja, em artigo/p/ISSN/ANO/ISSUE_ORDER/...,
    # os recupera e os ingressa junto aos registros da base artigo
    p_records = get_paragraphs_records(_id)
    doc = classic_website_migration.Document(records + p_records)
    isis_document = (
            db.fetch_isis_document(_id) or
            db.create_isis_document()
    )
    isis_document._id = _id
    isis_document.records = doc.records

    isis_document.doi = doc.doi
    isis_document.pub_year = doc.issue_publication_date[:4]

    isis_document.isis_updated_date = doc.updated_date
    isis_document.isis_created_date = doc.created_date
    isis_document.update_status("ISIS_METADATA_MIGRATED")

    isis_document.file_name = os.path.basename(doc.file_code)
    isis_document.file_type = (
        "xml" if doc.file_code.endswith(".xml") else "html"
    )
    isis_document.issue_folder = doc.issue_folder

    isis_journal = db.fetch_isis_journal(doc.journal_pid)
    journal = classic_website_migration.Journal(isis_journal.record)
    isis_document.acron = journal.acronym

    # salva o documento
    db.save_data(isis_document)
