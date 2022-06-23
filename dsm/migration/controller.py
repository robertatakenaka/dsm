
from scielo_classic_website.migration import (
    get_document_pids_to_migrate,
    get_records_by_pid,
    get_records_by_source_path,
    get_records_by_acron,
)

from dsm.migration import tasks


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
