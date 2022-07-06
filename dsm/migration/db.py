from mongoengine import Q

from dsm.core.db import fetch_record
from dsm import exceptions
from dsm.migration.models import (
    ISISDocument,
    ISISJournal,
    ISISIssue,
)


def fetch_isis_document(_id, **kwargs):
    return fetch_record(_id, ISISDocument, **kwargs)


def create_isis_document():
    try:
        return ISISDocument()
    except Exception as e:
        raise exceptions.DBCreateDocumentError(e)


def fetch_isis_journal(_id, **kwargs):
    return fetch_record(_id, ISISJournal, **kwargs)


def create_isis_journal():
    try:
        return ISISJournal()
    except Exception as e:
        raise exceptions.DBCreateDocumentError(e)


def fetch_isis_issue(_id, **kwargs):
    return fetch_record(_id, ISISIssue, **kwargs)


def create_isis_issue():
    try:
        return ISISIssue()
    except Exception as e:
        raise exceptions.DBCreateDocumentError(e)


def get_isis_documents_by_date_range(
        isis_updated_from=None, isis_updated_to=None):
    if isis_updated_from and isis_updated_to:
        return ISISDocument.objects(
            Q(isis_updated_date__gte=isis_updated_from) &
            Q(isis_updated_date__lte=isis_updated_to)
        )
    if isis_updated_from:
        return ISISDocument.objects(
            Q(isis_updated_date__gte=isis_updated_from)
        )
    if isis_updated_to:
        return ISISDocument.objects(
            Q(isis_updated_date__lte=isis_updated_to)
        )


def get_isis_documents_by_publication_year(year):
    return ISISDocument.objects(pub_year=year)


def get_isis_documents(acron=None, issue_folder=None, pub_year=None):
    params = {}
    if acron:
        params['acron'] = acron
    if pub_year:
        params['pub_year'] = pub_year
    if issue_folder:
        params['issue_folder'] = issue_folder

    if params:
        return ISISDocument.objects(**params)
    return []
