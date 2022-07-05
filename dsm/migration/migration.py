"""
Módulo que trata a migração

Coordena a interação entre:

- base de dados mongo com registros com dados do site clássico
- minio com arquivos do site clássico
- biblioteca scielo_migration (scielo_classic_website)

"""

from scielo_classic_website import migration as classic_website

from dsm.files_storage import migrated_files_storage
from dsm.migration import tasks, data_storage


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
    for doc in classic_website.get_document_pids(from_date, to_date):
        tasks.create_mininum_record_in_isis_doc(
            doc["pid"], doc["updated"]
        )


def register_isis_journal(_id, record):
    return data_storage.register_isis_journal(_id, record)


def register_isis_issue(_id, record):
    return data_storage.register_isis_issue(_id, record)


def register_isis_document(_id, record):
    return data_storage.register_isis_document(_id, record)


def register_isis_issue_files(acron, issue_folder):
    """
    Obtém os arquivos das pastas:
        classic_website/htdocs/img/revistas/<acron>/<issue_folder>
        classic_website/bases/pdf/<acron>/<issue_folder>
        classic_website/bases/xml/<acron>/<issue_folder>
        classic_website/bases/translation/<acron>/<issue_folder>
    Armazena-os no `minio`
    Registra seus dados no `mongodb`

    Parameters
    ----------
    acron: str
    issue_folder: str

    Returns
    -------
    None
    """
    issue_files = classic_website.get_issue_files(acron, issue_folder)

    imgs = None
    pdfs = None
    xmls = None
    htmls = None

    if issue_files.get("xml"):
        # registra no minio e obtém a uri do zip
        xmls = migrated_files_storage.store_issue_files(
            acron, issue_folder, issue_files["xml"], "xml")

    if issue_files.get("html"):
        # registra no minio e obtém a uri do zip
        htmls = migrated_files_storage.store_issue_files(
            acron, issue_folder, issue_files["html"], "html")

    if issue_files.get("pdf"):
        # registra no minio e obtém a uri do zip
        pdfs = migrated_files_storage.store_issue_files(
            acron, issue_folder, issue_files["pdf"], "pdf")

    if issue_files.get("img"):
        # registra no minio e obtém a uri do zip
        imgs = migrated_files_storage.store_issue_files(
            acron, issue_folder, issue_files["img"], "img")

    # registra na base de dados o caminho no minio e info dos arquivos
    return data_storage.register_isis_issue_files(
        acron, issue_folder,
        imgs, pdfs, xmls, htmls,
    )


def register_isis_img_issue_files(acron, issue_folder):
    """
    Obtém os arquivos das pastas:
        classic_website/htdocs/img/revistas/<acron>/<issue_folder>
    Armazena-os no `minio`
    Registra seus dados no `mongodb`

    Parameters
    ----------
    acron: str
    issue_folder: str

    Returns
    -------
    None
    """
    issue_files = classic_website.get_issue_files(acron, issue_folder)

    if issue_files.get("img"):
        imgs = migrated_files_storage.store_issue_files(
            acron, issue_folder, issue_files["img"], "img")

        return data_storage.register_isis_img_issue_files(
            acron, issue_folder, imgs,
        )


def register_isis_pdf_issue_files(acron, issue_folder):
    """
    Obtém os arquivos das pastas:
        classic_website/bases/pdf/<acron>/<issue_folder>
    Armazena-os no `minio`
    Registra seus dados no `mongodb`

    Parameters
    ----------
    acron: str
    issue_folder: str

    Returns
    -------
    None
    """
    issue_files = classic_website.get_issue_files(acron, issue_folder)

    if issue_files.get("pdf"):
        pdfs = migrated_files_storage.store_issue_files(
            acron, issue_folder, issue_files["pdf"], "pdf")

        return data_storage.register_isis_pdf_issue_files(
            acron, issue_folder, pdfs,
        )


def register_isis_xml_issue_files(acron, issue_folder):
    """
    Obtém os arquivos das pastas:
        classic_website/bases/xml/<acron>/<issue_folder>
    Armazena-os no `minio`
    Registra seus dados no `mongodb`

    Parameters
    ----------
    acron: str
    issue_folder: str

    Returns
    -------
    None
    """
    issue_files = classic_website.get_issue_files(acron, issue_folder)

    if issue_files.get("xml"):
        xmls = migrated_files_storage.store_issue_files(
            acron, issue_folder, issue_files["xml"], "xml")

        return data_storage.register_isis_xml_issue_files(
            acron, issue_folder, xmls,
        )


def register_isis_html_issue_files(acron, issue_folder):
    """
    Obtém os arquivos das pastas:
        classic_website/bases/translation/<acron>/<issue_folder>
    Armazena-os no `minio`
    Registra seus dados no `mongodb`

    Parameters
    ----------
    acron: str
    issue_folder: str

    Returns
    -------
    None
    """
    issue_files = classic_website.get_issue_files(acron, issue_folder)

    if issue_files.get("html"):
        htmls = migrated_files_storage.store_issue_files(
            acron, issue_folder, issue_files["html"], "html")

        return data_storage.register_isis_html_issue_files(
            acron, issue_folder, htmls,
        )


def get_migrated_pdf_files(acron, issue_folder, basename):
	data_storage.get_isis_issue_files(acron, issue_folder, file_type)

