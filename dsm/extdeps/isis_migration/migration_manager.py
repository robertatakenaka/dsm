import os
import glob

from scielo_v3_manager.v3_gen import generates

from dsm.utils.async_download import download_files
from dsm.utils.files import (
    create_zip_file, read_from_zipfile, create_temp_file,
)
from dsm.configuration import (
    BASES_XML_PATH,
    BASES_PDF_PATH,
    BASES_TRANSLATION_PATH,
    HTDOCS_IMG_REVISTAS_PATH,
    check_migration_sources,
    get_files_storage,
    get_db_url,
    get_files_storage_folder_for_pdfs,
)
from dsm.core.issue import get_bundle_id
from dsm.core.document import (
    set_translate_titles,
    set_translated_sections,
    set_abstracts,
    set_keywords,
    set_authors,
    set_authors_meta,
)
from dsm.core.document_files import (
    files_storage_register,
)
from dsm.extdeps.isis_migration import friendly_isis
from dsm.extdeps import db


class MigrationManager:
    """
    Obtém os dados das bases ISIS: artigo, title e issue.
    Registra-os, respectivamente nas coleções do mongodb:
    isis_doc, isis_journal, isis_issue

    Obtém os arquivos de:
    - bases/pdf
    - bases/xml
    - htdocs/img/revistas
    Faz um pacote zip com os arquivos de cada documento e os registra no minio

    # TODO
    # Obtém os dados das bases artigo/p/ (registros de parágrafos)

    # TODO
    #    Obtém os arquivos de:
    #     - bases/translation
    """
    def __init__(self):
        """
        Instancia objeto da classe MigrationManager
        """
        self._db_url = get_db_url()
        self._files_storage = get_files_storage()
        check_migration_sources()

    def db_connect(self):
        db.mk_connection(self._db_url)

    def register_isis_document(self, _id, records):
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

        doc = friendly_isis.FriendlyISISDocument(_id, records)
        isis_document = (
                db.fetch_isis_document(_id) or
                db.create_isis_document()
        )
        isis_document._id = doc._id
        isis_document.doi = doc.doi
        isis_document.pub_year = doc.collection_pubdate[:4]

        isis_document.isis_updated_date = doc.isis_updated_date
        isis_document.isis_created_date = doc.isis_created_date
        isis_document.records = doc.records
        isis_document.status = "1"

        # salva o documento
        return db.save_data(isis_document)

    def register_isis_journal(self, _id, record):
        """
        Register migrated journal data

        Parameters
        ----------
        _id: str
        record : JSON

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

        journal = friendly_isis.FriendlyISISJournal(_id, record)
        isis_journal = (
                db.fetch_isis_journal(_id) or
                db.create_isis_journal()
        )
        isis_journal._id = journal._id
        isis_journal.isis_updated_date = journal.isis_updated_date
        isis_journal.isis_created_date = journal.isis_created_date
        isis_journal.record = journal.record

        # salva o journal
        return db.save_data(isis_journal)

    def register_isis_issue(self, _id, record):
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

        issue = friendly_isis.FriendlyISISIssue(_id, record)
        isis_issue = (
                db.fetch_isis_issue(issue._id) or
                db.create_isis_issue()
        )
        isis_issue._id = issue._id
        isis_issue.isis_updated_date = issue.isis_updated_date
        isis_issue.isis_created_date = issue.isis_created_date
        isis_issue.record = issue.record

        # salva o issue
        return db.save_data(isis_issue)

    def update_website_journal_data(self, journal_id):
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
        i_journal = db.fetch_isis_journal(journal_id)

        # interface mais amigável para obter os dados
        fi_journal = friendly_isis.FriendlyISISJournal(
            i_journal._id, i_journal.record)

        # cria ou recupera o registro do website
        journal = (
            db.fetch_journal(journal_id) or db.create_journal()
        )

        # atualiza os dados
        _update_journal_with_isis_data(journal, fi_journal)

        # salva os dados
        return db.save_data(journal)

    def update_website_issue_data(self, issue_id):
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
        i_issue = db.fetch_isis_issue(issue_id)

        # interface mais amigável para obter os dados
        fi_issue = friendly_isis.FriendlyISISIssue(
            i_issue._id, i_issue.record)

        # cria ou recupera o registro do website
        issue = db.fetch_issue(issue_id) or db.create_issue()

        # atualiza os dados
        _update_issue_with_isis_data(issue, fi_issue)

        # salva os dados
        return db.save_data(issue)

    def update_website_document_metadata(self, document_id):
        """
        Update the website document

        Parameters
        ----------
        document_id : str

        Returns
        -------
        dict
        """
        # obtém os dados de artigo
        i_doc = db.fetch_isis_document(document_id)
        fi_doc = friendly_isis.FriendlyISISDocument(i_doc._id, i_doc.records)

        # obtém os dados de issue
        i_issue = db.fetch_isis_issue(fi_doc.issue_pid)
        fi_issue = friendly_isis.FriendlyISISIssue(i_issue._id, i_issue.record)
        fi_doc.issue = fi_issue

        # cria ou recupera o registro de documento do website
        document = db.fetch_document(document_id) or db.create_document()

        # cria ou recupera o registro de issue do website
        bundle_id = get_bundle_id(
            fi_doc.journal_pid,
            fi_doc.collection_pubdate[:4],
            fi_doc.volume,
            fi_doc.number,
            fi_doc.suppl,
        )
        issue = db.fetch_issue(bundle_id) or db.create_issue()

        # atualiza os dados
        _update_document_with_isis_data(document, fi_doc, issue)

        # salva os dados
        return db.save_data(document)

    def register_old_website_document_files(self, document_id):
        """
        Migrate document files

        Recover the files from
            BASES_XML_PATH,
            BASES_PDF_PATH,
            BASES_TRANSLATION_PATH,
            HTDOCS_IMG_REVISTAS_PATH,

        Parameters
        ----------
        document_id : str

        Returns
        -------
        dict
        """
        # registro migrado formato json
        i_doc = db.fetch_isis_document(document_id)
        f_doc = friendly_isis.FriendlyISISDocument(
            i_doc._id, i_doc.records)

        i_issue = db.fetch_isis_issue(f_doc.issue_pid)
        f_issue = friendly_isis.FriendlyISISIssue(
            i_issue._id, i_issue.record)

        i_journal = db.fetch_isis_journal(f_doc.journal_pid)
        f_journal = friendly_isis.FriendlyISISJournal(
            i_journal._id, i_journal.record)

        i_doc.file_name = f_doc.file_name
        i_doc.file_type = f_doc.file_type
        i_doc.acron = f_journal.acronym
        i_doc.issue_folder = f_issue.issue_folder

        files = _get_document_files(
            i_doc, f_doc.language, f_doc.journal_pid)

        # create zip file with document files
        zip_file_path = create_zip_file(files, i_doc.file_name + ".zip")

        # register in files storage (minio)
        files_storage_folder = os.path.join(
            "migration", f_doc.journal_pid, f_issue.issue_folder)
        _register_migrated_document_files_zipfile(
            self._files_storage, files_storage_folder, i_doc, zip_file_path)

        # salva os dados
        db.save_data(i_doc)
        return zip_file_path

    def list_documents(self, pub_year, updated_from, updated_to):
        """
        Migrate isis document data to website

        Parameters
        ----------
        pub_year: str
        updated_from: str
        update_to: str

        Returns
        -------
        dict
        """
        # registro migrado formato json
        print(f"{pub_year}, {updated_from}, {updated_to}")
        docs = []
        if pub_year:
            docs = db.get_isis_documents_by_publication_year(pub_year)
        elif updated_from or updated_to:
            docs = db.get_isis_documents_by_date_range(
                updated_from, updated_to)
            print(len(docs))
        return docs

    def update_website_document_pdfs(self, article_pid):
        """
        Update the website document pdfs

        Get texts from paragraphs records and from translations files
            registered in `isis_doc`
        Build the HTML files and register them in the files storage
        Update the `document.pdfs` with lang and uri

        Parameters
        ----------
        article_pid : str

        Returns
        -------
        dict
        """
        # obtém os dados de artigo
        migrated = MigratedDocument(article_pid)

        # cria ou recupera o registro de documento do website
        document = db.fetch_document(article_pid)
        if not document:
            raise exceptions.DocumentDoesNotExistError(
                "Document %s does not exist" % article_pid
            )

        # cria arquivos html com o conteúdo obtido dos arquivos html e
        # dos registros de parágrafos
        pdfs = []
        for pdf in migrated.pdfs:
            # obtém os conteúdos de html registrados em `isis_doc`
            file_path = create_temp_file(pdf["filename"], pdf["content"], "wb")
            # obtém a localização do arquivo no `files storage`
            folder = get_files_storage_folder_for_pdfs(
                migrated.journal_pid, migrated.issue_folder,
            )
            _pdf = {
                "lang": pdf["lang"],
                "type": "pdf",
                "filename": pdf["filename"],
            }
            try:
                # registra no files storage
                uri = self._files_storage.register(
                    file_path, folder, pdf["filename"], preserve_name=True
                )
                # atualiza com a uri o valor de pdfs
                _pdf.update({"url": uri})
            except:
                pass
            pdfs.append(_pdf)
        document.pdfs = pdfs

        # salva os dados
        return db.save_data(document)


def _get_document_files(f_doc, main_language, issn):
    """
    BASES_XML_PATH,
    BASES_PDF_PATH,
    BASES_TRANSLATION_PATH,
    HTDOCS_IMG_REVISTAS_PATH,
    """
    subdir_acron_issue_folder = os.path.join(
        f_doc.acron, f_doc.issue_folder)

    pdf_locations = _get_pdf_files_locations(
        subdir_acron_issue_folder, f_doc.file_name, main_language)
    _set_pdfs(f_doc, pdf_locations)

    asset_locations = _get_asset_files_locations(
        subdir_acron_issue_folder, f_doc.file_name)
    _set_assets(f_doc, asset_locations)

    # TODO
    # f_doc.translations = DictField()
    translations_locations = []
    xml_location = []
    if f_doc.file_type == "xml":
        xml = _get_xml_location(
            subdir_acron_issue_folder, f_doc.file_name)
        if xml:
            xml_location.append(xml)
    f_doc.status = "2"
    return (
        list(pdf_locations.values()) +
        asset_locations +
        xml_location +
        translations_locations
    )


def _register_migrated_document_files_zipfile(
        files_storage, files_storage_folder, f_doc, zip_file_path):
    try:
        uri_and_name = files_storage_register(
            files_storage,
            files_storage_folder,
            zip_file_path,
            os.path.basename(zip_file_path),
            preserve_name=True)
        f_doc.zipfile = db.create_remote_and_local_file(
            remote=uri_and_name["uri"], local=uri_and_name["name"])
        f_doc.status = "3"
    except Exception as e:
        # TODO melhorar retorno sobre registro de pacote zip
        print(e)


def _get_xml_location(subdir_acron_issue_folder, file_name):
    try:
        xml_file_path = os.path.join(
            BASES_XML_PATH,
            subdir_acron_issue_folder,
            f"{file_name}.xml"
        )
        return glob.glob(xml_file_path)[0]
    except IndexError:
        raise FileNotFoundError("Not found %s" % xml_file_path)


def _set_pdfs(f_doc, pdf_locations):
    pdfs = {}
    for lang, pdf_path in pdf_locations.items():
        pdfs[lang] = os.path.basename(pdf_path)
    f_doc.pdfs = pdfs


def _get_pdf_files_locations(subdir_acron_issue_folder, file_name, main_lang):
    files = {}
    for pattern in (f"{file_name}.pdf", f"??_{file_name}.pdf"):
        paths = glob.glob(
            os.path.join(
                BASES_PDF_PATH,
                subdir_acron_issue_folder,
                pattern
            )
        )
        if not paths:
            continue
        if "_" in pattern:
            # translations
            for path in paths:
                basename = os.path.basename(path)
                lang = basename[:2]
                files[lang] = path
        else:
            # main pdf
            files[main_lang] = paths[0]
    return files


def _set_assets(f_doc, asset_locations):
    f_doc.assets = [
        os.path.basename(asset_path)
        for asset_path in asset_locations
    ]


def _get_asset_files_locations(subdir_acron_issue_folder, file_name):
    return glob.glob(
        os.path.join(
            HTDOCS_IMG_REVISTAS_PATH,
            subdir_acron_issue_folder,
            f"{file_name}*.*"
        )
    )


def _update_document_with_isis_data(document, f_document, issue):
    """
    Update the `document` attributes with `f_document` attributes

    Parameters
    ----------
    document : opac_schema.v1.models.Document
    f_document : dsm.extdeps.isis_migration.friendly_isis.FriendlyISISDocument
    """
    # usa a data de criação do registro no isis como data de criação
    # do registro no site
    # TODO save_data sobrescreve estes comandos, refatorar
    document.created = db.convert_date(f_document.isis_created_date)
    document.updated = db.convert_date(f_document.isis_updated_date)

    _set_issue_data(document, issue)
    _set_order(document, f_document)
    # _set_renditions(document, registered_renditions)
    # _set_xml_url(document, registered_xml)
    _set_ids(document, f_document)
    _set_is_public(document, is_public=True)
    _set_languages(document, f_document)
    _set_article_abstracts(document, f_document)
    _set_article_authors(document, f_document)
    _set_article_pages(document, f_document)
    _set_article_publication_date(document, f_document)
    _set_article_sections(document, f_document)
    _set_article_titles(document, f_document)
    _set_article_type(document, f_document)


def _set_order(article, f_document):
    article.order = int(f_document.order)


def _set_renditions(article, renditions):
    # TODO
    article.pdfs = renditions


def _set_xml_url(article, xml_url):
    # TODO
    article.xml = xml_url


def _set_issue_data(article, issue):
    # TODO verificar se faz sentido neste local
    # if article.issue is not None and article.issue.number == "ahead":
    #     if article.aop_url_segs is None:
    #         url_segs = {
    #             "url_seg_article": article.url_segment,
    #             "url_seg_issue": article.issue.url_segment,
    #         }
    #         article.aop_url_segs = models.AOPUrlSegments(**url_segs)
    article.issue = issue
    article.journal = issue.journal


def _set_is_public(article, is_public=True):
    article.is_public = is_public


def _set_ids(article, f_document):
    article._id = (
        article._id or f_document.scielo_pid_v3 or generates()
    )
    article.aid = article._id

    article_pids = {}
    if f_document.scielo_pid_v1:
        article_pids["v1"] = f_document.scielo_pid_v1
    if f_document.scielo_pid_v2:
        article_pids["v2"] = f_document.scielo_pid_v2
    if f_document.scielo_pid_v3:
        article_pids["v3"] = f_document.scielo_pid_v3
    article.scielo_pids = article_pids

    article.aop_pid = f_document.ahead_of_print_pid
    article.pid = f_document.scielo_pid_v2
    article.doi = f_document.doi

    # TODO
    # doi por idioma / mudanca no opac_schema


def _set_article_type(article, f_document):
    article.type = f_document.article_type


def _set_languages(article, f_document):
    article.original_language = f_document.language
    article.languages = f_document.languages
    article.htmls = [{"lang": lang} for lang in f_document.languages]


def _set_article_titles(article, f_document):
    article.title = f_document.original_title
    set_translate_titles(article, f_document.translated_titles)


def _set_article_sections(article, f_document):
    article.section = f_document.original_section
    set_translated_sections(article, f_document.translated_sections)


def _set_article_abstracts(article, f_document):
    article.abstract = f_document.abstract
    set_abstracts(article, f_document.abstracts)
    set_keywords(article, f_document.keywords_groups)


def _set_article_publication_date(article, f_document):
    article.publication_date = f_document.document_pubdate


def _set_article_pages(article, f_document):
    article.elocation = f_document.elocation_id
    article.fpage = f_document.fpage
    article.fpage_sequence = f_document.fpage_seq
    article.lpage = f_document.lpage


def _set_article_authors(article, f_document):
    set_authors(article, f_document.contrib_group)
    set_authors_meta(article, f_document.contrib_group)


def _update_issue_with_isis_data(issue, f_issue):
    """
    Update the `issue` attributes with `f_issue` attributes
    Parameters
    ----------
    issue : opac_schema.v1.models.Issue
    f_issue : dsm.extdeps.isis_migration.friendly_isis.FriendlyISISIssue
    """
    # TODO issue._id deve ter o mesmo padrão usado no site novo
    # e não o pid de fascículo do site antigo
    issue.iid = f_issue.iid

    issue.journal = db.fetch_journal(f_issue.journal_pid)
    # ReferenceField(Journal, reverse_delete_rule=CASCADE)

    # not available in isis
    # TODO: verificar o uso no site
    # issue.cover_url = f_issue.cover_url

    if f_issue.number == "ahead":
        issue.volume = None
        issue.number = None
    else:
        issue.volume = f_issue.volume
        # TODO: verificar o uso no site | números especiais spe_text
        issue.number = f_issue.number

    # nao presente na base isis
    # TODO: verificar o uso no site
    issue.type = _get_issue_type(f_issue)

    # supplement
    issue.suppl_text = f_issue.suppl

    # nao presente na base isis // tem uso no site?
    # TODO: verificar o uso no site
    issue.spe_text = f_issue.spe_text

    # nao presente na base isis // tem uso no site?
    issue.start_month = f_issue.start_month

    # nao presente na base isis // tem uso no site?
    issue.end_month = f_issue.end_month

    issue.year = int(f_issue.year)
    issue.label = f_issue.issue_folder

    # TODO: no banco do site 20103 como int e isso está incorreto
    # TODO: verificar o uso no site
    # ou fica como str 20130003 ou como int 3
    issue.order = int(f_issue.order)

    issue.is_public = f_issue.is_public

    # nao presente na base isis // tem uso no site?
    # issue.unpublish_reason = f_issue.unpublish_reason

    # PID no site antigo
    issue.pid = f_issue.pid

    # isso é atribuído no pre_save do modelo
    # issue.url_segment = f_issue.url_segment

    # nao presente na base isis // tem uso no site?
    # issue.assets_code = f_issue.assets_code

    # ID no site
    issue._id = get_bundle_id(
        issue.journal._id,
        issue.year,
        issue.volume,
        issue.number,
        issue.suppl_text,
    )


def _get_issue_type(f_issue):
    """
    https://github.com/scieloorg/opac-airflow/blob/1064b818fda91f73414a6393d364663bdefa9665/airflow/dags/sync_kernel_to_website.py#L511
    https://github.com/scieloorg/opac_proc/blob/3c6bd66040de596e1af86a99cca6d205bfb79a68/opac_proc/transformers/tr_issues.py#L76
    'ahead', 'regular', 'special', 'supplement', 'volume_issue'
    """
    if f_issue.suppl:
        return "supplement"
    if f_issue.number:
        if f_issue.number == "ahead":
            return "ahead"
        if "spe" in f_issue.number:
            return "special"
        return "regular"
    return "volume_issue"


def _update_journal_with_isis_data(journal, f_journal):
    """
    Update the `journal` attributes with `f_journal` attributes
    Parameters
    ----------
    journal : opac_schema.v1.models.Journal
    f_journal : dsm.extdeps.isis_migration.friendly_isis.FriendlyISISJournal
    """
    journal._id = f_journal._id
    journal.jid = f_journal._id

    # TODO
    # journal.collection = f_journal.attr
    # ReferenceField(Collection, reverse_delete_rule=CASCADE)

    # TODO
    # journal.timeline = f_journal.attr
    # EmbeddedDocumentListField(Timeline)

    journal.subject_categories = f_journal.subject_categories

    journal.study_areas = f_journal.study_areas

    # TODO
    # journal.social_networks = f_journal.attr
    # EmbeddedDocumentListField(SocialNetwork)

    journal.title = f_journal.title

    journal.title_iso = f_journal.iso_abbreviated_title

    # TODO verificar conteúdo
    journal.next_title = f_journal.new_title
    journal.short_title = f_journal.abbreviated_title

    journal.acronym = f_journal.acronym

    journal.scielo_issn = f_journal._id
    journal.print_issn = f_journal.print_issn
    journal.eletronic_issn = f_journal.electronic_issn

    journal.subject_descriptors = f_journal.subject_descriptors
    journal.copyrighter = f_journal.copyright_holder
    journal.online_submission_url = f_journal.online_submission_url

    # ausente na base isis
    # journal.title_slug = f_journal.attr
    # journal.logo_url = f_journal.attr

    # TODO
    if f_journal.current_status == "C":
        journal.current_status = "current"
    journal.editor_email = f_journal.email

    journal.index_at = f_journal.index_at

    journal.is_public = f_journal.publication_status == 'C'

    journal.mission = [
        db.models.Mission(**{"language": lang, "description": text})
        for lang, text in f_journal.mission.items()
    ]

    # TODO EmbeddedDocumentListField(OtherTitle)
    # nao usa no site?
    # journal.other_titles = f_journal.other_titles

    # TODO ID or title
    # journal.previous_journal_ref = f_journal.previous_journal_title

    journal.publisher_address = f_journal.publisher_address
    journal.publisher_city = f_journal.publisher_city
    journal.publisher_state = f_journal.publisher_state
    journal.publisher_country = f_journal.publisher_country

    # TODO ver na base de dados do site
    journal.publisher_name = f_journal.get_publisher_names()

    # journal.scimago_id = f_journal.scimago_id

    journal.sponsors = f_journal.sponsors

    journal.study_areas = f_journal.study_areas

    # journal.timeline = f_journal.timeline

    journal.unpublish_reason = f_journal.unpublish_reason

    # journal.url_segment = f_journal.url_segment


class MigratedDocument:

    def __init__(self, _id):
        self._id = _id
        self._isis_document = db.fetch_isis_document(_id)
        if not self._isis_document:
            raise exceptions.DBFetchDocumentError("%s is not migrated" % _id)
        self._f_doc = friendly_isis.FriendlyISISDocument(
            _id, self._isis_document.records)
        self._zipfile_path = None
        try:
            if self._isis_document.zipfile:
                self._zipfile_path = download_files(
                    [self._isis_document.zipfile])[0]
                print(self._zipfile_path)
        except:
            print("Unable to get %s" % self._isis_document.zipfile)
            raise

    @property
    def file_name(self):
        return self._isis_document.file_name

    @property
    def issue_folder(self):
        return self._isis_document.issue_folder

    @property
    def acron(self):
        return self._isis_document.acron

    @property
    def journal_pid(self):
        return self._id[1:10]

    @property
    def html_paragraphs(self):
        return friendly_isis.FriendlyISISParagraphs(
            self._isis_document._id,
            self._isis_document.records)

    @html_paragraphs.setter
    def html_paragraphs(self, p_records):
        """
        Register migrated document html paragraphs, if apply

        Parameters
        ----------
        _id: str

        Returns
        -------
        str
            _id

        Raises
        ------
            TypeError
            PermissionError
        """
        if self._isis_document.file_type == "xml":
            raise TypeError("XML does not have HTML paragraphs")
        if not p_records:
            raise ValueError(
                "Unable to set html_paragraphs: paragraph records not found")
        doc_paragraphs = friendly_isis.FriendlyISISParagraphs(
            self._isis_document._id,
            self._isis_document.records)
        doc_paragraphs.replace_paragraphs(p_records)

    @property
    def pdfs(self):
        _pdfs = []
        if self._zipfile_path:
            for lang, file in self._isis_document.pdfs.items():
                _pdf = {}
                _pdf["lang"] = lang
                _pdf["filename"] = file
                _pdf["type"] = "pdf"
                _pdf["content"] = read_from_zipfile(self._zipfile_path, file)
                _pdfs.append(_pdf)
        return _pdfs

    @property
    def translations(self):
        return self._isis_document.translations

    @translations.setter
    def translations(self, translations_paths):
        _translations = {}
        for lang, translation_path in translations_paths.items():
            _translations[lang] = [
                os.path.basename(path) for path in translation_path]
        self._isis_document.translations = _translations

    @property
    def html_texts(self):
        if self._isis_document.file_type == "xml":
            return []
        texts = []
        paragraphs = self.html_paragraphs
        text = {
            "lang": self._f_doc.language,
            "filename": self._isis_document.file_name + ".html",
            "text": paragraphs.text,
        }
        texts.append(text)

        for transl_text in self.translated_texts:
            text = {
                "lang": transl_text["lang"],
                "filename": transl_text["filename"],
            }
            text["text"] = transl_text["text"][0]
            if paragraphs.references:
                text["text"] += "".join(paragraphs.references)
            if len(transl_text["text"]) > 1:
                text["text"] += transl_text["text"][1]
            texts.append(text)
        return texts

    @property
    def translated_texts(self):
        texts = []
        if self._isis_document.translations and self._isis_document.zipfile:
            try:
                zipfile_path = download_files([self._isis_document.zipfile])[0]
            except:
                print("Unable to get %s" % zipfile_path)
            else:
                for lang, files in self._isis_document.translations.items():
                    text = {}
                    text["lang"] = lang
                    text["filename"] = files[0] + ".html"
                    text["text"] = []
                    for file in files:
                        text["text"].append(
                            read_from_zipfile(
                                zipfile_path, file
                            ).decode("iso-8859-1")
                        )
                    texts.append(text)
        return texts

    def save(self):
        # salva o documento
        return db.save_data(self._isis_document)
