from opac_schema.v1 import models

from dsm.core.db import save_data
from dsm.new_website import db


def format_yyyy_hyphen_mm_hyphen_dd(yyyymmdd):
    # FIXME
    if '-' in yyyymmdd:
        return yyyymmdd
    # Returns yyyy-mm-dd
    parts = [yyyymmdd[:4], yyyymmdd[4:6], yyyymmdd[6:]]
    checked = []
    for part in parts:
        if int(part):
            checked.append(part)
    return "-".join(checked)


def format_author_name(surname, given_names, suffix):
    # like airflow
    surname_and_suffix = surname
    if suffix:
        surname_and_suffix += " " + suffix
    return "%s%s, %s" % (surname_and_suffix, given_names)


def get_document(pid, aop_pid):
    # FIXME
    # cria ou recupera o registro de documento do website
    if aop_pid:
        return (
            db.fetch_document(aop_pid=aop_pid) or
            db.fetch_document(pid=aop_pid) or
            db.fetch_document(pid=pid) or
            db.create_document()
        )
    if pid:
        return (
            db.fetch_document(pid=pid) or
            db.create_document()
        )


def publish_document(document):
    document.aid = document._id
    save_data(document)


def update_data_string_type(
        document,
        pid,
        v3,
        main_article_title,
        main_language,
        main_abstract,
        main_doi,
        aop_pid,
        publication_date,
        type,
        elocation,
        fpage,
        fpage_sequence,
        lpage,
        toc_section,
        ):
    document._id = v3
    document.aid = v3
    document.title = main_article_title
    document.section = toc_section
    document.abstract = main_abstract
    document.doi = main_doi
    document.pid = pid
    document.aop_pid = aop_pid
    document.original_language = main_language
    document.publication_date = (
        format_yyyy_hyphen_mm_hyphen_dd(publication_date)
    )
    document.type = type
    document.elocation = elocation
    document.fpage = fpage
    document.fpage_sequence = fpage_sequence
    document.lpage = lpage


def add_issue(document, issue):
    # issue = ReferenceField(Issue, reverse_delete_rule=CASCADE))
    document.issue = issue


def add_journal(document, journal):
    # journal = ReferenceField(Journal, reverse_delete_rule=CASCADE))
    document.journal = journal


def add_translated_title(document, text, language):
    # translated_titles = EmbeddedDocumentListField(TranslatedTitle))
    if document.translated_titles is None:
        document.translated_titles = []
    _translated_title = models.TranslatedTitle()
    _translated_title.name = text
    _translated_title.language = language
    document.translated_titles.append(_translated_title)


def add_section(document, text, language):
    # sections = EmbeddedDocumentListField(TranslatedSection))
    if document.translated_sections is None:
        document.translated_sections = []
    _translated_section = models.TranslatedSection()
    _translated_section.name = text
    _translated_section.language = language
    document.translated_sections.append(_translated_section)


def add_author_meta(document, surname, given_names, suffix, affiliation, orcid):
    # authors_meta = EmbeddedDocumentListField(AuthorMeta))
    if document.authors_meta is None:
        document.authors_meta = []
    author = models.AuthorMeta()
    author.surname = surname
    author.given_names = given_names
    author.suffix = suffix
    author.affiliation = affiliation
    author.orcid = orcid
    document.authors_meta.append(author)


def add_abstract(document, text, language):
    # abstracts = EmbeddedDocumentListField(Abstract))
    if document.abstracts is None:
        document.abstracts = []
    _abstract = models.Abstract()
    _abstract.text = text
    _abstract.language = language
    document.abstracts.append(_abstract)


def add_doi_with_lang(document, doi, language):
    # doi_with_lang = EmbeddedDocumentListField(DOIWithLang))
    if document.doi_with_lang_items is None:
        document.doi_with_lang_items = []
    _doi_with_lang_item = models.DOIWithLang()
    _doi_with_lang_item.doi = doi
    _doi_with_lang_item.language = language
    document.doi_with_lang_items.append(_doi_with_lang_item)


def add_mat_suppl(document, url, lang, ref_id, filename):
    # mat_suppl = EmbeddedDocumentListField(MatSuppl))
    if document.mat_suppl_items is None:
        document.mat_suppl_items = []
    _mat_suppl_item = models.MatSuppl()
    _mat_suppl_item.url = url
    _mat_suppl_item.lang = lang
    _mat_suppl_item.ref_id = ref_id
    _mat_suppl_item.filename = filename
    document.mat_suppl_items.append(_mat_suppl_item)


def add_keywords(document, lang, keywords):
    # kwd_groups = EmbeddedDocumentListField(ArticleKeyword))
    if document.kwd_groups is None:
        document.kwd_groups = []
    _kwd_group = models.ArticleKeyword()
    _kwd_group.lang = lang
    _kwd_group.keywords = keywords
    document.kwd_groups.append(_kwd_group)


def add_related_article(document, doi, ref_id, related_type):
    # related_article = EmbeddedDocumentListField(MatSuppl))
    if document.related_articles is None:
        document.related_articles = []
    _related_article = models.RelatedArticle()
    _related_article.doi = doi
    _related_article.ref_id = ref_id
    _related_article.related_type = related_type
    document.related_articles.append(_related_article)


def add_author(document, surname, given_names, suffix):
    # authors = EmbeddedDocumentListField(Author))
    if document.authors is None:
        document.authors = []
    _author = format_author_name(
        surname, given_names, suffix,
    )
    document.authors.append(_author)


def add_language(document, language):
    # ListField()
    if document.languages is None:
        document.languages = []
    document.languages.append(language)


def add_abstract_languages(document):
    document.abstract_languages = [
        abstract["language"] for abstract in document.abstracts
    ]


def add_html(document, language, uri):
    # FIXME htmls = ListField(field=DictField()))
    if document.htmls is None:
        document.htmls = []
    document.htmls.append({"lang": language, "uri": uri})


def add_pdf(document, lang, url, filename, type):
    # FIXME pdfs = ListField(field=DictField()))
    """
    {
        "lang": rendition["lang"],
        "url": rendition["url"],
        "filename": rendition["filename"],
        "type": "pdf",
    }
    """
    if document.pdfs is None:
        document.pdfs = []
    document.pdfs.append(
        dict(
            lang=lang,
            url=url,
            filename=filename,
            type=type,
        )
    )


def add_is_aop(document, is_aop):
    # FIXME is_aop = BooleanField()
    document.is_aop = bool(is_aop)


def add_is_public(document, is_public):
    # is_public = BooleanField(required=True, default=True))
    document.is_public = bool(is_public)


def add_display_full_text(document, display_full_text):
    # display_full_text = BooleanField(required=True, default=True))
    document.display_full_text = bool(display_full_text)


def add_order(document, order):
    # order = IntField())
    document.order = int(order)


def add_aop_url_segs(document, aop_url_segs):
    # FIXME aop_url_segs = EmbeddedDocumentField(AOPUrlSegments))
    document.aop_url_segs = aop_url_segs


def add_xml(document, xml):
    # FIXME xml
    document.xml = xml


def add_domain_key(document, domain_key):
    # FIXME domain_key
    document.domain_key = domain_key


def add_scielo_pids(document, v1=None, v2=None, v3=None, others=None):
    # FIXME scielo_pids = DictField())
    scielo_pids = {}
    if v1:
        scielo_pids['v1'] = v1
    if v2:
        scielo_pids['v2'] = v2
    if v3:
        scielo_pids['v3'] = v3
    if others:
        scielo_pids['other'] = others
    document.scielo_pids = scielo_pids
