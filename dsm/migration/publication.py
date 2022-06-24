from scielo_classic_website.migration import (
    get_document_pids_to_migrate,
    get_paragraphs_id_file_path,
)
from scielo_classic_website import migration as classic_website_migration
from dsm.new_website.journal import update_journal
from dsm.new_website.issue import update_issue

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


def adapt_issue_data(issue_data):
    data = {}
    if issue_data.number == "ahead":
        data["volume"] = None
        data["number"] = None
    data["suppl_text"] = issue_data.get("supplement_volume") or issue_data.get("supplement_number")

    # FIXME
    data["spe_text"] = (
        issue_data["number"] if 'spe' in issue_data["number"] else None
    )
    data["year"] = int(issue_data["publication_date"][:4])
    data["label"] = issue_data["issue_folder"]
    data["assets_code"] = issue_data["issue_folder"]

    # TODO: no banco do site 20103 como int e isso está incorreto
    # TODO: verificar o uso no site
    # ou fica como str 20130003 ou como int 3
    data["order"] = int(issue_data["order"][4:])
    return data


def publish_issue_data(issue_id):
    """
    Migrate isis issue data to website

    Parameters
    ----------
    issue_id : str

    Returns
    -------
    dict
    """
    # registro migrado formato json
    isis_registered = db.fetch_isis_issue(issue_id)

    # interface mais amigável para obter os dados
    issue = classic_website_migration.Issue(isis_registered.record)
    issue_data = issue.attributes
    issue_data.update(adapt_issue_data(isis_registered))

    # cria ou recupera o registro do website
    registered_issue = db.fetch_issue(issue_id) or db.create_issue()

    # atualiza os dados
    update_issue(registered_issue, issue_data)

    # salva os dados
    db.save_data(registered_issue)
