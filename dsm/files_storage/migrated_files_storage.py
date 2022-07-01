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


def get_document_files(acron, issue_folder, file_type_folder, zipfile_name, filenames=None):
    """
    Recupera os arquivos do site clássico

    Parameters
    ----------
    acron: str
    issue_folder: str
    file_type_folder: str (xml, pdf, html, img)
    zipfile_name: str (basename of zip_file_path)
    filenames: str list (lista dos arquivos)

    """
    subdirs = get_subdirs(acron, issue_folder, file_type_folder)
    path = os.path.join(subdirs, zipfile_name)
    zip_path = classic_website_files_storage.get_file(path)
    files = {}
    with ZipFile(zip_path) as zf:
        filenames = filenames or zf.namelist()
        for filename in filenames:
            with zf.open(filename, "rb") as fp:
                files[filename] = fp.read()
    return files
