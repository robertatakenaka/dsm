from scielo_classic_website.migration import (
    get_document_pids_to_migrate,
    get_paragraphs_id_file_path,
)
from scielo_classic_website import migration as classic_website_migration
from dsm.new_website.journal import update_journal

from dsm.migration import db


def adapt_journal_data(original):
    return dict(
        title_iso=original.abbreviated_iso_title,
    )


def publish_journal_data(journal_id):
    """
    Migrate isis journal data to website

    Parameters
    ----------
    journal_id : str

    Returns
    -------
    dict
    """
    # registro migrado formato json
    journal_isis = db.fetch_isis_journal(journal_id)

    # interface mais amigável para obter os dados
    journal_i = classic_website_migration.Journal(journal_isis.record)

    journal_data = journal_i.attributes
    journal_data.update(adapt_journal_data(journal_data))

    # cria ou recupera o registro do new website
    journal = (
        db.fetch_journal(journal_id) or db.create_journal()
    )

    # atualiza os dados
    update_journal(journal, journal_data)

    # salva os dados
    db.save_data(journal)
