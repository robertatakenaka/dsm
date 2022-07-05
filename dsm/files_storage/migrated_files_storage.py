import os
from zipfile import ZipFile

from dsm import configuration


classic_website_files_storage = configuration.get_files_storage(
    configuration.MINIO_BUCKET_SUBDIR_CLASSIC_WEBSITE)


def get_subdirs(acron, issue_folder, folder):
    """
    Returns <folder>/<acron>/<issue_folder> de
        https://minio.domain/classic/<folder>/<acron>/<issue_folder>
    """
    return "/".join([
        folder,
        acron, issue_folder,
    ])


def store_issue_files(acron, issue_folder, files, folder):
    subdirs = get_subdirs(acron, issue_folder, folder)

    uri = classic_website_files_storage.register(
        files["zip_file_path"],
        subdirs=subdirs,
        preserve_name=True,
    )
    return dict(
        zipfile_uri=uri,
        zipfile_name=os.path.basename(files["zip_file_path"]),
        content_info=files["files"],
    )


def get_issue_files(acron, issue_folder, folder, zipfile_name, file_names=None):
    """
    Recupera do minio os arquivos do site clássico

    Parameters
    ----------
    acron: str
    issue_folder: str
    folder: str (xml, pdf, html, img)
    zipfile_name: str (basename of zip_file_path)
    file_names: str list (lista dos arquivos a serem obtidos)
        se None, retorna todos items do zip
    """
    if folder not in ("xml", "pdf", "html", "img"):
        raise ValueError(
            "migrate_document_files.get_document_files expects "
            "xml or pdf or html or img as value for "
            "`folder` parameter"
        )
    subdirs = get_subdirs(acron, issue_folder, folder)
    path = os.path.join(subdirs, zipfile_name)
    return classic_website_files_storage.get_zip_file_items(path)
