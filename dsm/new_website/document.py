from opac_schema.v1 import models

from dsm.migration import db
from dsm.new_website import exceptions


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
    db.save_data(document)


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


def add_translated_titles(document, translated_titles):
    # translated_titles = EmbeddedDocumentListField(TranslatedTitle))
    _translated_titles = []
    for translated_title in translated_titles:
        _translated_title = models.TranslatedTitle()
        _translated_title.name = translated_title['name']
        _translated_title.language = translated_title['language']
        _translated_titles.append(_translated_title)
    document.translated_titles = _translated_titles


def add_sections(document, translated_sections):
    # sections = EmbeddedDocumentListField(TranslatedSection))
    _translated_sections = []
    for translated_section in translated_sections:
        _translated_section = models.TranslatedSection()
        _translated_section.name = translated_section['name']
        _translated_section.language = translated_section['language']
        _translated_sections.append(_translated_section)
    document.translated_sections = _translated_sections


def add_authors_meta(document, authors):
    # authors_meta = EmbeddedDocumentListField(AuthorMeta))
    _authors = []
    for author in authors:
        _author = models.AuthorMeta()
        _author.name = format_author_name(
            author['surname'], author['given_names'], author.get("suffix"),
        )
        _author.affiliation = author['affiliation']
        _author.orcid = author['orcid']
        _authors.append(_author)
    document.authors_meta = _authors


def add_abstracts(document, abstracts):
    # abstracts = EmbeddedDocumentListField(Abstract))
    _abstracts = []
    for abstract in abstracts:
        _abstract = models.Abstract()
        _abstract.language = abstract['language']
        _abstract.text = abstract['text']
        _abstracts.append(_abstract)
    document.abstracts = _abstracts


def add_doi_with_lang(document, doi_with_lang_items):
    # doi_with_lang = EmbeddedDocumentListField(DOIWithLang))
    _doi_with_lang_items = []
    for doi_with_lang_item in doi_with_lang_items:
        _doi_with_lang_item = models.DOIWithLang()
        _doi_with_lang_item.language = doi_with_lang_item['language']
        _doi_with_lang_item.doi = doi_with_lang_item['doi']
        _doi_with_lang_items.append(_doi_with_lang_item)
    document.doi_with_lang_items = _doi_with_lang_items


def add_mat_suppl(document, mat_suppl_items):
    # mat_suppl = EmbeddedDocumentListField(MatSuppl))
    _mat_suppl_items = []
    for mat_suppl_item in mat_suppl_items:
        _mat_suppl_item = models.MatSuppl()
        _mat_suppl_item.lang = mat_suppl_item['lang']
        _mat_suppl_item.url = mat_suppl_item['url']
        _mat_suppl_item.ref_id = mat_suppl_item['ref_id']
        _mat_suppl_item.filename = mat_suppl_item['filename']
        _mat_suppl_items.append(_mat_suppl_item)
    document.mat_suppl_items = _mat_suppl_items


def add_keywords(document, kwd_groups):
    # kwd_groups = EmbeddedDocumentListField(ArticleKeyword))
    _kwd_groups = []
    for kwd_group in kwd_groups:
        _kwd_group = models.ArticleKeyword()
        _kwd_group.lang = kwd_group['lang']
        _kwd_group.keywords = kwd_group['keywords']
        _kwd_groups.append(_kwd_group)
    document.kwd_groups = _kwd_groups


def add_related_articles(document, related_articles):
    # related_articles = EmbeddedDocumentListField(RelatedArticle))
    _related_articles = []
    for related_article in related_articles:
        _related_article = models.RelatedArticle()
        _related_article.ref_id = related_article['ref_id']
        _related_article.doi = related_article['doi']
        _related_article.related_type = related_article['related_type']
        _related_articles.append(_related_article)
    document.related_articles = _related_articles


def add_authors(document, authors):
    # authors_meta = EmbeddedDocumentListField(AuthorMeta))
    _authors = []
    for author in authors:
        _author = format_author_name(
            author['surname'], author['given_names'], author.get("suffix"),
        )
        _authors.append(_author)
    document.authors = _authors


def add_languages(document, languages):
    # ListField()
    document.languages = languages


def add_abstract_languages(document):
    document.abstract_languages = [
        abstract["language"] for abstract in document.abstracts
    ]


def add_htmls(document, htmls):
    # FIXME htmls = ListField(field=DictField()))
    for html in htmls:
        for k in ("lang", ):
            try:
                x = html[k]
            except KeyError:
                raise exceptions.PDFValueError(
                    "Adding html to document: %s. Missing %s" % (html, k)
                )
    document.htmls = htmls


def add_pdfs(document, pdfs):
    # FIXME pdfs = ListField(field=DictField()))
    """
    {
        "lang": rendition["lang"],
        "url": rendition["url"],
        "filename": rendition["filename"],
        "type": "pdf",
    }
    """
    for pdf in pdfs:
        for k in ("lang", "url", "filename", "type"):
            try:
                x = pdf[k]
            except KeyError:
                raise exceptions.PDFValueError(
                    "Adding pdf to document: %s. Missing %s" % (pdf, k)
                )
    document.pdfs = pdfs


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
