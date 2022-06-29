from scielo_classic_website.migration import (
    get_document_pids_to_migrate,
    get_paragraphs_id_file_path,
)
from scielo_classic_website import migration as classic_website_migration
from dsm.new_website.journal import update_journal
from dsm.new_website.issue import update_issue
from dsm.new_website import document as new_website_document_publisher

from dsm.migration.migrated_document import MigratedDocument

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


def adapt_document_data(original):
    data = {}
    data['abstracts'] = original['abstracts']
    data['acceptance_date_iso'] = original['acceptance_date_iso']
    data['affiliations'] = original['affiliations']
    data['ahead_publication_date'] = original['ahead_publication_date']
    data['any_issn'] = original['any_issn']
    data['aop_pid'] = original['aop_pid']
    data['article_titles'] = original['article_titles']
    data['article_type'] = original['article_type']
    data['assets_code'] = original['assets_code']
    data['authors'] = original['authors']
    data['collection_acronym'] = original['collection_acronym']
    data['contract'] = original['contract']
    data['corporative_authors'] = original['corporative_authors']
    data['creation_date'] = original['creation_date']
    data['data_model_version'] = original['data_model_version']
    data['document_publication_date'] = original['document_publication_date']
    data['document_type'] = original['document_type']
    data['doi'] = original['doi']
    data['doi_with_lang'] = original['doi_with_lang']
    data['file_code'] = original['file_code']
    data['fulltexts'] = original['fulltexts']
    data['html_url'] = original['html_url']
    data['illustrative_material'] = original['illustrative_material']
    data['internal_sequence_id'] = original['internal_sequence_id']
    data['is_ahead_of_print'] = original['is_ahead_of_print']
    data['issue'] = original['issue']
    data['issue_label'] = original['issue_label']
    data['issue_number'] = original['issue_number']
    data['issue_publication_date'] = original['issue_publication_date']
    data['issue_url'] = original['issue_url']
    data['journal'] = original['journal']
    data['journal_id'] = original['journal_id']
    data['keywords'] = original['keywords']
    data['languages'] = original['languages']
    data['mixed_affiliations'] = original['mixed_affiliations']
    data['normalized_affiliations'] = original['normalized_affiliations']
    data['num_suppl'] = original['num_suppl']
    data['order'] = original['order']
    data['original_html'] = original['original_html']
    data['original_language'] = original['original_language']
    data['original_section'] = original['original_section']
    data['page'] = original['page']
    data['pdf_url'] = original['pdf_url']
    data['permissions'] = original['permissions']
    data['processing_date'] = original['processing_date']
    data['project_name'] = original['project_name']
    data['project_sponsor'] = original['project_sponsor']
    data['publication_date'] = original['publication_date']
    data['publisher_ahead_id'] = original['publisher_ahead_id']
    data['publisher_id'] = original['publisher_id']
    data['receive_date_iso'] = original['receive_date_iso']
    data['review_date_iso'] = original['review_date_iso']
    data['scielo_domain'] = original['scielo_domain']
    data['scielo_pid_v2'] = original['scielo_pid_v2']
    data['scielo_pid_v3'] = original['scielo_pid_v3']
    data['section'] = original['section']
    data['section_code'] = original['section_code']
    data['thesis_degree'] = original['thesis_degree']
    data['thesis_organization'] = original['thesis_organization']
    data['translated_htmls'] = original['translated_htmls']
    data['translated_section'] = original['translated_section']
    data['update_date'] = original['update_date']
    data['vol_suppl'] = original['vol_suppl']
    data['volume'] = original['volume']
    data['xml_languages'] = original['xml_languages']
    return data


def publish_document_metadata(pid_v2):
    """
    Update the website document

    Parameters
    ----------
    pid_v2 : str

    Returns
    -------
    dict
    """
    # FIXME domain_key?
    # obtém os dados de artigo migrado
    migrated_document = MigratedDocument(pid_v2)

    classic_website_doc = migrated_document.classic_doc

    # atualiza os dados
    document = new_website_document_publisher.get_document(
        pid_v2, classic_website_doc.aop_pid)
    migrate_metadata(document, classic_website_doc)

    new_website_document_publisher.publish_document(document)

    # indica status de `published_incomplete`
    migrated_document._isis_document.update_status("PUBLISHED_INCOMPLETE")

    # mas ao salvar o documento, este será avaliado se os arquivos que
    # deveriam ser publicados estão publicados, então o status muda
    # para `published_complete`
    db.save_data(migrated_document._isis_document)


def migrate_metadata(document, classic_website_doc):
    """
    Update the website `document` with `classic_website_doc` metadata

    Parameters
    ----------
    document : opac_schema.v1.models.Article
    classic_website_doc : scielo_classic_website.migration.document.Document

    Returns
    -------
    dict
    """
    # FIXME domain_key?
    # obtém os dados de artigo migrado
    new_website_document_publisher.update_data_string_type(
        document,
        classic_website_doc.scielo_pid_v2,
        classic_website_doc.scielo_pid_v3,
        classic_website_doc.original_title,
        classic_website_doc.original_language,
        classic_website_doc.original_abstract,
        classic_website_doc.doi,
        classic_website_doc.aop_pid,
        classic_website_doc.document_publication_date,
        classic_website_doc.article_type,
        classic_website_doc.elocation,
        classic_website_doc.fpage,
        classic_website_doc.fpage_seq,
        classic_website_doc.lpage,
        classic_website_doc.original_section,
    )
    new_website_document_publisher.add_abstracts(
        document, classic_website_doc.abstracts)
    new_website_document_publisher.add_abstract_languages(document)
    new_website_document_publisher.add_authors(
        document, classic_website_doc.authors)
    new_website_document_publisher.add_authors_meta(
        document, classic_website_doc.authors)

    new_website_document_publisher.add_keywords(
        document, classic_website_doc.keywords_groups)
    new_website_document_publisher.add_order(
        document, classic_website_doc.order)
    new_website_document_publisher.add_translated_titles(
        document, classic_website_doc.translated_titles)
    # FIXME
    # new_website_document_publisher.add_related_articles(
    #     document, classic_website_doc.related_articles)


def add_files():
    pass

    # new_website_document_publisher.add_xml(
    #     document, classic_website_doc.xml)
    # new_website_document_publisher.add_mat_suppl(
    #     document, classic_website_doc.mat_suppl_items)
    # new_website_document_publisher.add_pdfs(
    #     document, classic_website_doc.pdfs)
    # new_website_document_publisher.add_htmls(
    #     document, classic_website_doc.htmls)
    # new_website_document_publisher.add_languages(
    #     document, classic_website_doc.languages)
    # new_website_document_publisher.add_doi_with_lang(
    #     document, classic_website_doc.doi_with_lang_items)


def migrate_other_data():
    pass
    # FIXME site
    # new_website_document_publisher.add_aop_url_segs(
    #     document, classic_website_doc.aop_url_segs)
    # new_website_document_publisher.add_display_full_text(
    #     document, classic_website_doc.display_full_text)
    # new_website_document_publisher.add_domain_key(
    #     document, classic_website_doc.domain_key)
    # new_website_document_publisher.add_is_public(
    #     document, classic_website_doc.is_public)

    # FIXME pids
    # new_website_document_publisher.add_scielo_pids(
    #     document, classic_website_doc.v1,
    #     pid_v2,
    #     classic_website_doc.scielo_pid_v3,
    #     classic_website_doc.others,
    # )

    # FIXME aop
    # new_website_document_publisher.add_is_aop(
    #     document, classic_website_doc.is_aop)

    # FIXME
    # new_website_document_publisher.add_journal(
    #     document, classic_website_doc.journal)

    # FIXME
    # new_website_document_publisher.add_issue(
    #     document, classic_website_doc.issue)
    # new_website_document_publisher.add_sections(
    #     document, classic_website_doc.translated_sections)
