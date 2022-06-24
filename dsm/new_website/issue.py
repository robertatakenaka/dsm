from opac_schema.v1.models import (
    Journal,
    Timeline,
    SocialNetwork,
    OtherTitle,
    Mission,
    JounalMetrics,
    LastIssue,
)

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


def update_issue(registered, data):
    """
    Update the `issue` attributes with `data` attributes
    Parameters
    ----------
    registered : opac_schema.v1.models.Issue
    data : dict
    """
    # TODO registered._id deve ter o mesmo padrão usado no site novo
    # e não o pid de fascículo do site antigo

    registered.journal = Journal.objects.filter(_id=data["journal_id"])
    # ReferenceField(Journal, reverse_delete_rule=CASCADE)

    # not available in isis
    # TODO: verificar o uso no site
    # registered.cover_url = f_registered.cover_url

    for attr_name in ISSUE_ATTRIBUTES:
        try:
            setattr(registered, attr_name, data[attr_name])
        except KeyError:
            pass

    registered.type = _get_issue_type(registered)

    # ID no site
    registered._id = get_bundle_id(
        registered.journal._id,
        registered.year,
        registered.volume,
        registered.number,
        registered.suppl_text,
    )
    registered.iid = registered._id
    registered.is_public = True

    return registered


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
