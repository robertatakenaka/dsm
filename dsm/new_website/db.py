from opac_schema.v1 import models
from opac_schema.v2 import models as v2_models

from dsm import exceptions
from dsm.core.db import fetch_record, fetch_records, save_data


def fetch_document(any_doc_id, **kwargs):
    try:
        articles = models.Article.objects(
            pk=any_doc_id, **kwargs)
        if not articles:
            articles = models.Article.objects(
                pid=any_doc_id, **kwargs)
        if not articles:
            articles = models.Article.objects(
                aop_pid=any_doc_id, **kwargs)
        if not articles:
            articles = models.Article.objects(
                scielo_pids__other=any_doc_id, **kwargs)
        if not articles:
            articles = models.Article.objects(
                doi=any_doc_id, **kwargs)
        return articles and articles[0]
    except Exception as e:
        raise exceptions.DBFetchDocumentError(e)


def fetch_articles_files(**kwargs):
    return v2_models.ArticleFiles.objects(**kwargs)


def fetch_journals(**kwargs):
    return fetch_records(models.Journal, **kwargs)


def fetch_issues(**kwargs):
    return fetch_records(models.Issue, **kwargs)


def fetch_documents(**kwargs):
    return fetch_records(models.Article, **kwargs)


def fetch_journal(_id, **kwargs):
    return fetch_record(_id, models.Journal, **kwargs)


def fetch_issue(_id, **kwargs):
    return fetch_record(_id, models.Issue, **kwargs)


def create_document():
    try:
        return models.Article()
    except Exception as e:
        raise exceptions.DBCreateDocumentError(e)


def create_journal():
    try:
        return models.Journal()
    except Exception as e:
        raise exceptions.DBCreateDocumentError(e)


def create_issue():
    try:
        return models.Issue()
    except Exception as e:
        raise exceptions.DBCreateDocumentError(e)


def create_remote_and_local_file(remote, local, annotation=None):
    try:
        file = {}
        if local:
            file["name"] = local
        if remote:
            file["uri"] = remote
        if annotation:
            file["annotation"] = annotation
        return v2_models.RemoteAndLocalFile(**file)
    except Exception as e:
        raise exceptions.RemoteAndLocalFileError(
            "Unable to create RemoteAndLocalFile(%s, %s): %s" %
            (remote, local, e)
        )


def register_received_package(_id, uri, name, annotation=None):
    received = v2_models.ReceivedPackage()
    received._id = _id
    received.file = create_remote_and_local_file(uri, name, annotation)
    return save_data(received)


def fetch_document_packages(v3):
    return v2_models.ArticleFiles.objects(aid=v3).order_by('-updated')


def fetch_document_package_by_pid_and_version(pid, version):
    return v2_models.ArticleFiles.objects.get(
        aid=pid,
        version=version
    )


def register_document_package(v3, data):
    """
    data = {}
    data['xml'] = xml_uri_and_name
    data['assets'] = assets
    data['renditions'] = renditions
    data['file'] = file
    """
    article_files = v2_models.ArticleFiles()
    article_files.aid = v3
    article_files.version = _get_article_files_new_version(v3)
    article_files.scielo_pids = {'v3': v3}

    _set_document_package_file_paths(article_files, data)
    save_data(article_files)

    return article_files


def _get_article_files_new_version(v3):
    current_version = fetch_document_packages(v3).count() or 0
    return current_version + 1


def _set_document_package_file_paths(article_files, data):
    article_files.xml = create_remote_and_local_file(
        data['xml']['uri'],
        data['xml']['name']
    )

    article_files.file = create_remote_and_local_file(
        data['file']['uri'],
        data['file']['name']
    )

    assets = []
    for item in data["assets"]:
        assets.append(
            create_remote_and_local_file(
                item['uri'],
                item['name']
            )
        )
    article_files.assets = assets

    renditions = []
    for item in data["renditions"]:
        renditions.append(
            create_remote_and_local_file(
                item['uri'],
                item['name']
            )
        )
    article_files.renditions = renditions
