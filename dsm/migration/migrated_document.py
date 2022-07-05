"""
Rascunho / talvez não será usado
"""

from scielo_classic_website import migration as classic_website_migration

from dsm.migration import db, exceptions

from scielo_classic_website.config import DocumentFiles
from scielo_classic_website.models.html_body import (
    BodyFromISIS,
    BodyFromHTMLFile,
)


class MigratedDocument:

    def __init__(self, document):
        self.document = document

        self._document_files = DocumentFiles(
            os.path.join(
                self.document.journal.acron, self.document.issue_folder),
            self.document.filename, self.document.original_language)
        self._files_storage_folder = get_files_storage_folder_for_migration(
            self.journal_pid, self.document.issue_folder, self.file_name
        )

    @property
    def html_translation_texts(self):
        """
        Obtém lang, filename e text (front, reference, back) de cada idioma
        do arquivo HTML de tradução
        """
        references_text = self.document.body_from_isis.references_text
        for lang, front_and_back_files in self.html_translations_files.items():
            body_from_html_file = BodyFromHTMLFile(
                front_and_back_files["front"],
                self.document.body_from_isis.references_text,
                front_and_back_files.get("back"),
            )
            yield {
                "lang": lang,
                "filename": os.path.basename(front_and_back_files["front"]),
                "text": body_from_html_file.html,
            }


class OldMigratedDocument:

    def __init__(self, _id):
        self._id = _id
        self._isis_document = db.fetch_isis_document(_id)
        self._isis_issue = db.fetch_isis_issue(_id[1:18])

        if not self.isis_doc:
            raise exceptions.DBFetchDocumentError("%s is not migrated" % _id)
        self.classic_doc = classic_website_migration.Document(self.isis_doc.records)
        self.classic_doc.issue = classic_website_migration.Issue(self._isis_issue.record)

        self._document_files = DocumentFilesAtOldWebsite(
            os.path.join(
                self.isis_doc.acron, self.isis_doc.issue_folder),
            self.isis_doc.file_name, self.classic_doc.language)
        self._files_storage_folder = get_files_storage_folder_for_migration(
            self.journal_pid, self.isis_doc.issue_folder, self.file_name
        )
        self.tracker = None
        self.files_to_zip = []
        self._assets_location = {}

    @property
    def isis_doc(self):
        return self._isis_document

    @property
    def pub_year(self):
        return self.classic_doc.collection_pubdate[:4]

    @property
    def volume(self):
        return self.classic_doc.volume

    @property
    def number(self):
        return self.classic_doc.number

    @property
    def suppl(self):
        return self.classic_doc.suppl

    @property
    def file_name(self):
        return self.isis_doc.file_name

    @property
    def issue_folder(self):
        return self.isis_doc.issue_folder

    @property
    def acron(self):
        return self.isis_doc.acron

    @property
    def journal_pid(self):
        return self.isis_doc.journal_pid

    @property
    def translations(self):
        return self.isis_doc.translations

    @property
    def html_translation_texts(self):
        """
        Obtém lang, filename e text (front, reference, back) de cada idioma
        do arquivo HTML de tradução
        """
        paragraphs = self.classic_doc.paragraphs
        translations = {}
        for lang, front_and_back_files in self.html_translations_files.items():
            yield {
                "lang": lang,
                "filename": os.path.basename(front_and_back_files["front"]),
                "text": build_translation_html_text(
                    front_and_back_files, paragraphs.references,
                ),
            }

    @property
    def html_texts(self):
        if self.isis_doc.file_type == "xml":
            return []

        paragraphs = self.classic_doc.paragraphs
        yield {
            "lang": self.classic_doc.language,
            "filename": self.isis_doc.file_name + ".html",
            "text": paragraphs.text or "",
        }
        for text in self.html_translation_texts:
            yield text

    @property
    def html_texts_to_publish(self):
        """
        Obtém lang, filename e text de cada idioma do HTML adequado para
        publicação no site, ou seja, os ativos digitais localizados no Files
        Storage
        """
        if self.isis_doc.file_type == "xml":
            return []
        # ativos digitais agrupado pelo idioma do HTML
        assets_by_lang = {}
        for asset in self.isis_doc.asset_files:
            lang = asset["annotation"]["lang"]
            assets_by_lang.setdefault(lang, [])
            assets_by_lang[lang].append(
                {
                    "elem": asset["annotation"]["elem"],
                    "attr": asset["annotation"]["attr"],
                    "original": asset["annotation"]["original"],
                    "new": asset["uri"],
                }
            )
        # para cada artigo no formato de HTML
        for html_text in self.html_texts:
            # obtém os ativos digitais no texto HTML do idioma correspondente
            assets = assets_by_lang.get(html_text["lang"])

            # retorna o HTML com os caminhos das imagens conforme
            # registrados no Files Storage
            yield {
                "lang": html_text["lang"],
                "text": adapt_html_text_to_website(
                    html_text["text"],
                    assets
                ),
                "filename": html_text["filename"],
            }

    def xml_texts_to_publish(self, document):
        if self.isis_doc.file_type == "html":
            return []
        texts = []
        try:
            content = read_file(self.xml_file_path)
            sps_pkg = SPS_Package(content)
            sps_pkg.local_to_remote(self.isis_doc.asset_files)
            sps_pkg.scielo_pid_v3 = document._id
            sps_pkg.scielo_pid_v2 = document.pid
            if document.aop_pid:
                sps_pkg.aop_pid = document.aop_pid
            content = sps_pkg.xml_content
        except Exception as e:
            # TODO melhorar o tratamento de excecao
            text = {
                "lang": self.classic_doc.language,
                "filename": self.isis_doc.file_name + ".xml",
                "error": (
                    f"Unable to get XML to publish {self.xml_file_path}: {e}"
                ),
            }
        else:
            text = {
                "lang": self.classic_doc.language,
                "filename": self.isis_doc.file_name + ".xml",
                "text": content,
                "languages": [{"lang": lang} for lang in sps_pkg.languages],
            }
        texts.append(text)
        return texts

    def save(self):
        # salva o documento
        return db.save_data(self.isis_doc)

    @property
    def original_pdf_paths(self):
        """
        Obtém os arquivos PDFs do documento da pasta BASES_PDF_PATH
        """
        for lang, pdf_path in self._document_files.bases_pdf_files_paths.items():
            yield {
                "path": pdf_path,
                "lang": lang,
                "basename": os.path.basename(pdf_path)
            }

    def _migrate_document_file(self, files_storage, file_path, basename, annotation=None):

        self.tracker.info(f"migrate {file_path}")

        # identificar para inserir no zip do pacote
        self.files_to_zip.append(file_path)

        try:
            # registra o arquivo na nuvem
            remote = files_storage.register(
                file_path, self._files_storage_folder,
                basename, preserve_name=True)

            self.tracker.info(f"migrated {remote}")
        except Exception as e:
            self.tracker.error(
                f"Unable to register {file_path} in files storage: {e}"
            )
        else:
            return db.create_remote_and_local_file(
                remote, basename, annotation)

    def migrate_pdfs(self, files_storage):
        """
        Obtém os arquivos PDFs do documento da pasta BASES_PDF_PATH
        Registra os arquivos na nuvem
        Atualiza os dados de PDF de `isis_document`
        """
        pdfs = {}
        _uris_and_names = []

        for pdf in self.original_pdf_paths:
            file_path = pdf["path"]
            lang = pdf["lang"]

            pdfs[lang] = pdf["basename"]
            migrated = self._migrate_document_file(files_storage, file_path, pdf["basename"])
            if migrated:
                _uris_and_names.append(migrated)

        self.isis_doc.pdfs = pdfs
        self.isis_doc.pdf_files = _uris_and_names

    @property
    def migrated_pdfs(self):
        # url, filename, type, lang
        _pdfs = []
        uris = {
            item.name: item.uri
            for item in self.isis_doc.pdf_files
        }
        for lang, name in self.isis_doc.pdfs.items():
            _pdfs.append(
                {
                    "lang": lang,
                    "filename": name,
                    "url": uris.get(name),
                    "type": "pdf",
                }
            )
        return _pdfs

    def migrate_images_from_folder(self, files_storage):
        """
        Obtém os arquivos de ativos digitais da pasta HTDOCS_IMG_REVISTAS_PATH
        Registra os arquivos na nuvem
        Atualiza os dados dos ativos digitais em `isis_document`
        """
        if self.isis_doc.file_type != "xml":
            return
        _files = []
        _uris_and_names = []
        for file_path in self._document_files.htdocs_img_revistas_files_paths:
            name = os.path.basename(file_path)
            migrated = self._migrate_document_file(
                files_storage, file_path, name)
            if migrated:
                _uris_and_names.append(migrated)
            _files.append(name)
        self.isis_doc.assets = _files
        self.isis_doc.asset_files = _uris_and_names

    @property
    def assets_location(self):
        """
        A partir dos ativos digitais encontrados no HTML, localiza-os
        no sistema de arquivos
        """
        if not self._assets_location:
            self._assets_location = {}
            HTDOCS_PATH = get_htdocs_path()
            for text in self.html_texts:
                if not text["text"]:
                    self.tracker.error(
                        f"html {text['filename']} ({text['lang']}) is empty")
                    continue

                self._assets_location[text['lang']] = []
                for asset in get_assets_locations(text["text"]):
                    # fullpath
                    subdir = asset["path"]
                    if subdir.startswith("/"):
                        subdir = subdir[1:]
                    if "img/revistas" in subdir and not subdir.startswith("img"):
                        subdir = subdir[subdir.find("img"):]
                    file_path = os.path.realpath(
                        os.path.join(HTDOCS_PATH, subdir))
                    self._assets_location[text['lang']].append(
                        {
                            "original": asset["link"],
                            "elem": asset["elem"].tag,
                            "attr": asset["attr"],
                            "file_path": file_path,
                        }
                    )
        return self._assets_location

    def migrate_images_from_html(self, files_storage):
        """
        Parse HTML content to get src / href
        Obtém os arquivos de ativos digitais da pasta HTDOCS_IMG_REVISTAS_PATH
        Registra os arquivos na nuvem
        Atualiza os dados dos ativos digitais em `isis_document`

        Some images in html content might be located in an unexpected path,
        such as, /img/revista/acron/nahead/, althouth the document is not aop
        anymore.
        Some images might have not same preffix name as the main html file,
        for instance, main file name is a01.htm, their images can be named as
        a1f1.
        This attribute have to parse the HTML and recover the images from
        /img/revista/ located in unexpected folders and with unexpected file
        names.

        Returns
        -------
        dict
        """
        if self.isis_doc.file_type != "html":
            return
        htmls = []
        _files = []
        _uris_and_names = []

        for lang, assets in self.assets_location.items():

            for asset in assets:
                self.tracker.info(f"migrate html ({lang}): {asset}")
                file_path = asset["file_path"]

                # basename
                basename = os.path.basename(file_path)
                _files.append(basename)

                if not os.path.isfile(file_path):
                    self.tracker.error(f"Not found {file_path}")
                    continue

                annotation = {
                    "original": asset["original"],
                    "elem": asset["elem"],
                    "attr": asset["attr"],
                    "lang": lang,
                }
                migrated = self._migrate_document_file(
                    files_storage, file_path, basename, annotation)
                if migrated:
                    _uris_and_names.append(migrated)

        self.isis_doc.assets = _files
        self.isis_doc.asset_files = _uris_and_names

    @property
    def html_translations_files(self):
        return self._document_files.bases_translation_files_paths

    @property
    def xml_file_path(self):
        return self._document_files.bases_xml_file_path

    def migrate_text_files(self, files_storage):
        """
        Obtém os arquivos que correspondem aos textos completos das pastas
        BASES_XML_PATH,
        BASES_TRANSLATION_PATH,
        Registra os arquivos na nuvem
        Atualiza os dados de texto completo de `isis_document`
        """
        _uris_and_names = []

        self.isis_doc.translations = {}
        self.isis_doc.xml_files = []
        self.isis_doc.html_files = []

        if self.isis_doc.file_type == "xml":
            file_path = self.xml_file_path
            self.tracker.info(f"migrate {file_path}")
            if file_path:
                migrated = self._migrate_document_file(
                    files_storage, file_path, os.path.basename(file_path))
                if migrated:
                    _uris_and_names.append(migrated)
                    self.isis_doc.xml_files = _uris_and_names
            else:
                self.tracker.error(f"Not found {file_path}")
        else:
            # HTML Traduções
            _translations = {}
            for lang, front_and_back in self.html_translations_files.items():
                _translations[lang] = {}
                for label, file_path in front_and_back.items():
                    # HTML front or back filename
                    _translations[lang][label] = os.path.basename(file_path)
                    # upload to the cloud
                    self.tracker.info(f"migrate {file_path}")
                    # armazena o original
                    migrated = self._migrate_document_file(
                        files_storage, file_path, _translations[lang][label])
                    if migrated:
                        _uris_and_names.append(migrated)
            self.isis_doc.html_files = _uris_and_names
            self.isis_doc.translations = _translations

    def register_migrated_document_files_zipfile(self, files_storage):
        # create zip file with document files
        self.tracker.info(f"total of files to zip: {len(self.files_to_zip)}")
        zip_file_path = create_zip_file(
            self.files_to_zip, self.isis_doc.file_name + ".zip")
        remote = files_storage.register(
            zip_file_path,
            self._files_storage_folder,
            os.path.basename(zip_file_path),
            preserve_name=True)
        self.isis_doc.zipfile = db.create_remote_and_local_file(
            remote=remote, local=os.path.basename(zip_file_path),
            annotation={
                "files": [
                    os.path.basename(f) for f in self.files_to_zip
                ]
            })
        self.tracker.info(f"total of zipped files: {len(self.files_to_zip)}")
        return zip_file_path