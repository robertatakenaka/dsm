
from scielo_classic_website.isis_cmds import (
    create_id_file,
    get_id_file_path,
    get_document_isis_db,
    get_document_pids_to_migrate,
)

from dsm.migration import tasks, _controller


def identify_documents_to_migrate(from_date, to_date):
    for doc in get_document_pids_to_migrate(from_date, to_date):
        tasks.create_mininum_record_in_isis_doc(
            doc["pid"], doc["updated"]
        )
