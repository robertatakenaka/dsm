"""
Módulo componente da migração

Interface para as funções de migração que devem interagir com a interface Web
"""

from dsm.migration import manager, migration


def list_documents_to_migrate(
        acron, issue_folder, pub_year, isis_updated_from, isis_updated_to,
        status=None,
        descending=None,
        page_number=None,
        items_per_page=None,
        ):
    return migration.list_documents_to_migrate(
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
    pids_and_their_records = migration.get_records_by_pid(pid)
    return manager.migrate_isis_records(pids_and_their_records, "artigo")


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
    pids_and_their_records = migration.get_records_by_source_path(
        db_type, source_file_path)
    return manager.migrate_isis_records(pids_and_their_records, db_type)


def migrate_acron(acron, id_folder_path=None):
    pids_and_their_records = migration.get_records_by_acron(acron, id_folder_path)
    return manager.migrate_isis_records(pids_and_their_records, "artigo")


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
    return migration.identify_documents_to_migrate(from_date, to_date)


def migrate_issue_files(acron, issue_folder):
    return migration.register_isis_issue_files(acron, issue_folder)


def migrate_img_issue_files(acron, issue_folder):
    return migration.register_isis_img_issue_files(acron, issue_folder)


def migrate_pdf_issue_files(acron, issue_folder):
    return migration.register_isis_pdf_issue_files(acron, issue_folder)


def migrate_html_issue_files(acron, issue_folder):
    return migration.register_isis_html_issue_files(acron, issue_folder)


def migrate_xml_issue_files(acron, issue_folder):
    return migration.register_isis_xml_issue_files(acron, issue_folder)
