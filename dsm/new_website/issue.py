from opac_schema.v1.models import (
    Journal,
    Timeline,
    SocialNetwork,
    OtherTitle,
    Mission,
    JounalMetrics,
    LastIssue,
)

from dsm.new_website import db


ISSUE_ATTRIBUTES = (
    'cover_url',
    'volume',
    'number',
    'type',
    'suppl_text',
    'spe_text',
    'label',
    'unpublish_reason',
    'pid',
    'url_segment',
    'assets_code',
    'year',
    'order',
    'start_month',
    'end_month',
    'is_public',
)


def update_issue(data):
    """
    Update the `issue` attributes with `data` attributes
    Parameters
    ----------
    data : dict
    """

    new_website_issue_id = get_bundle_id(
        data['journal'],
        data['year'],
        data['volume'],
        data['number'],
        data['suppl_text'],
    )

    # obtém o registro issue do site novo
    published_issue = db.fetch_issue(new_website_issue_id) or db.create_issue()

    # TODO registered._id deve ter o mesmo padrão usado no site novo
    # e não o pid de fascículo do site antigo

    published_issue.journal = db.fetch_journal(_id=data["journal"])
    # ReferenceField(Journal, reverse_delete_rule=CASCADE)

    # not available in isis
    # TODO: verificar o uso no site
    # published_issue.cover_url = f_published_issue.cover_url

    for attr_name in ISSUE_ATTRIBUTES:
        try:
            setattr(published_issue, attr_name, data[attr_name])
        except KeyError:
            pass

    published_issue.type = _get_issue_type(published_issue)

    # ID no site
    published_issue._id = new_website_issue_id
    published_issue.iid = published_issue._id
    published_issue.is_public = True

    published_issue.pid = (
        published_issue.journal._id + str(published_issue.year) +
        str(published_issue.order).zfill(4)
    )
    if published_issue.type == "ahead":
        published_issue.url_segment = "9999.nahead"
    else:
        published_issue.url_segment = (
            f"{str(published_issue.year)}.{published_issue.label}"
        )

    return published_issue


def get_bundle_id(issn_id, year, volume=None, number=None, supplement=None):
    """
        Gera Id utilizado na ferramenta de migração
        para cadastro do documentsbundle.
    """
    items = (
        ("v", volume),
        ("n", number),
        ("s", supplement),
    )
    label = "-".join([f"{prefix}{value}"
                      for prefix, value in items
                      if value])
    if label:
        return f"{issn_id}-{year}-{label}"
    return f"{issn_id}-aop"


def _get_issue_type(registered):
    """
    https://github.com/scieloorg/opac-airflow/blob/1064b818fda91f73414a6393d364663bdefa9665/airflow/dags/sync_kernel_to_website.py#L511
    https://github.com/scieloorg/opac_proc/blob/3c6bd66040de596e1af86a99cca6d205bfb79a68/opac_proc/transformers/tr_issues.py#L76
    'ahead', 'regular', 'special', 'supplement', 'volume_issue'
    """
    if registered.suppl_text:
        return "supplement"
    if registered.spe_text:
        return "special"
    if registered.number:
        if "spe" in registered.number:
            return "special"
        return "regular"
    if registered.volume:
        return "volume_issue"
    return "ahead"
